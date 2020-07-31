package fr.acinq.eclair.blockchain.bitcoins

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{Actor, ActorLogging, Cancellable, Stash, Terminated}
import akka.pattern.pipe
import fr.acinq.bitcoin.{ByteVector32, OutPoint, Transaction}
import fr.acinq.eclair.KamonExt
import fr.acinq.eclair.blockchain.Monitoring.Metrics
import fr.acinq.eclair.blockchain.bitcoind.ZmqWatcher.{TickNewBlock, addWatchedUtxos, removeWatchedUtxos}
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.channel.BITCOIN_PARENT_TX_CONFIRMED
import fr.acinq.eclair.transactions.Scripts
import org.bitcoins.wallet.Wallet
import scodec.bits.ByteVector

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class BitcoinSWatcher(blockCount: AtomicLong)(implicit ec: ExecutionContext = ExecutionContext.global) extends Actor with Stash with ActorLogging {

  case class TriggerEvent(w: Watch, e: WatchEvent)
  case class WalletStarted(wallet: Wallet)

  def receive: Receive = uninitialized()

  def uninitialized(): Receive = {
    case wallet: BitcoinSWallet =>
      // this is to initialize block count
      self ! TickNewBlock
      context become watching(wallet, Set(), Map(), SortedMap(), None)
//      context become stopped(wallet)
  }

//  def stopped(wallet: Wallet): Receive = {
//    case walletStarted: WalletStarted
//    // this is to initialize block count
//    self ! TickNewBlock
//    context become watching(wallet, Set(), Map(), SortedMap(), None)
//  }

  def watching(wallet: BitcoinSWallet, watches: Set[Watch], watchedUtxos: Map[OutPoint, Set[Watch]], block2tx: SortedMap[Long, Seq[Transaction]], nextTick: Option[Cancellable]): Receive = {
    case NewTransaction(tx) =>
      log.debug("analyzing txid={} tx={}", tx.txid, tx)
      tx.txIn
        .map(_.outPoint)
        .flatMap(watchedUtxos.get)
        .flatten // List[Watch] -> Watch
        .collect {
          case w: WatchSpentBasic =>
            self ! TriggerEvent(w, WatchEventSpentBasic(w.event))
          case w: WatchSpent =>
            self ! TriggerEvent(w, WatchEventSpent(w.event, tx))
        }

    case NewBlockHeader(header) =>
      // using a Try because in tests we generate fake blocks
      log.debug("received blockid={}", Try(header.blockId).getOrElse(ByteVector32(ByteVector.empty)))
      nextTick.map(_.cancel()) // this may fail or succeed, worse case scenario we will have two ticks in a row (no big deal)
      log.debug("scheduling a new task to check on tx confirmations")
      // we do this to avoid herd effects in testing when generating a lots of blocks in a row
      val task = context.system.scheduler.scheduleOnce(2 seconds, self, TickNewBlock)
      context become watching(wallet, watches, watchedUtxos, block2tx, Some(task))

    case TickNewBlock =>
      val publishedEvent = for {
        blockHash <- wallet.chainQueryApi.getBestBlockHash()
        countOpt <- wallet.chainQueryApi.getBlockHeight(blockHash)
        count = countOpt.get.toInt
      } yield {
        log.debug("setting blockCount={}", count)
        blockCount.set(count)
        context.system.eventStream.publish(CurrentBlockCount(count))
      }
      publishedEvent.failed.foreach { ex =>
          log.error("Cannot publish current block count", ex)
      }

      val updatedBalance = for {
        balanceBTC <- wallet.getBalance
        // We track our balance in mBTC because a rounding issue in BTC would be too impactful.
        balance = balanceBTC.confirmed.toMilliBtc + balanceBTC.unconfirmed.toMilliBtc
      } yield {
        Metrics.BitcoinBalance.withoutTags().update(balance.toDouble)
      }
      updatedBalance.failed.foreach { ex =>
          log.error("Cannot update balance", ex)
      }

      // TODO: beware of the herd effect
      val updatedMetrics = KamonExt.timeFuture(Metrics.NewBlockCheckConfirmedDuration.withoutTags()) {
        Future.sequence(watches.collect { case w: WatchConfirmed => checkConfirmed(wallet, w) })
      }

      updatedMetrics.failed.foreach { ex =>
        log.error("Cannot publish current block count", ex)
      }
      context become watching(wallet, watches, watchedUtxos, block2tx, None)

    case TriggerEvent(w, e) if watches.contains(w) =>
      log.info(s"triggering $w")
      w.channel ! e
      w match {
        case _: WatchSpent =>
          // NB: WatchSpent are permanent because we need to detect multiple spending of the funding tx
          // They are never cleaned up but it is not a big deal for now (1 channel == 1 watch)
          ()
        case _ =>
          context become watching(wallet, watches - w, removeWatchedUtxos(watchedUtxos, w), block2tx, None)
      }

    case CurrentBlockCount(count) =>
      val toPublish = block2tx.filterKeys(_ <= count)
      toPublish.values.flatten.foreach(tx => publish(wallet, tx))
      context become watching(wallet, watches, watchedUtxos, block2tx -- toPublish.keys, None)

    case w: Watch if !watches.contains(w) =>
      w match {
        case WatchSpentBasic(_, txid, outputIndex, _, _) =>
          // not: we assume parent tx was published, we just need to make sure this particular output has not been spent
          wallet.isTransactionOutputSpendable(txid, outputIndex, includeMempool = true).collect {
            case false =>
              log.info(s"output=$outputIndex of txid=$txid has already been spent")
              self ! TriggerEvent(w, WatchEventSpentBasic(w.event))
          }

        case WatchSpent(_, txid, outputIndex, _, _) =>
          // first let's see if the parent tx was published or not
          wallet.getTxConfirmations(txid).collect {
            case Some(_) =>
              // parent tx was published, we need to make sure this particular output has not been spent
              wallet.isTransactionOutputSpendable(txid, outputIndex, includeMempool = true).collect {
                case false =>
                  log.info(s"$txid:$outputIndex has already been spent, looking for the spending tx in the mempool")
                  wallet.getMempool().map { mempoolTxs =>
                    mempoolTxs.filter(tx => tx.txIn.exists(i => i.outPoint.txid == txid && i.outPoint.index == outputIndex)) match {
                      case Nil =>
                        log.warning(s"$txid:$outputIndex has already been spent, spending tx not in the mempool, looking in the blockchain...")
                        wallet.lookForSpendingTx(None, txid, outputIndex).map { tx =>
                          log.warning(s"found the spending tx of $txid:$outputIndex in the blockchain: txid=${tx.txid}")
                          self ! NewTransaction(tx)
                        }
                      case txs =>
                        log.info(s"found ${txs.size} txs spending $txid:$outputIndex in the mempool: txids=${txs.map(_.txid).mkString(",")}")
                        txs.foreach(tx => self ! NewTransaction(tx))
                    }
                  }
              }
          }

        case w: WatchConfirmed => checkConfirmed(wallet, w) // maybe the tx is already tx, in that case the watch will be triggered and removed immediately

        case _: WatchLost => () // TODO: not implemented

        case w => log.warning(s"ignoring $w")
      }

      log.debug("adding watch {} for {}", w, sender)
      context.watch(w.channel)
      context become watching(wallet, watches + w, addWatchedUtxos(watchedUtxos, w), block2tx, nextTick)

    case PublishAsap(tx) =>
      val blockCount = this.blockCount.get()
      val cltvTimeout = Scripts.cltvTimeout(tx)
      val csvTimeout = Scripts.csvTimeout(tx)
      if (csvTimeout > 0) {
        require(tx.txIn.size == 1, s"watcher only supports tx with 1 input, this tx has ${tx.txIn.size} inputs")
        val parentTxid = tx.txIn.head.outPoint.txid
        log.info(s"txid=${tx.txid} has a relative timeout of $csvTimeout blocks, watching parenttxid=$parentTxid tx=$tx")
        val parentPublicKey = fr.acinq.bitcoin.Script.write(fr.acinq.bitcoin.Script.pay2wsh(tx.txIn.head.witness.stack.last))
        self ! WatchConfirmed(self, parentTxid, parentPublicKey, minDepth = 1, BITCOIN_PARENT_TX_CONFIRMED(tx))
      } else if (cltvTimeout > blockCount) {
        log.info(s"delaying publication of txid=${tx.txid} until block=$cltvTimeout (curblock=$blockCount)")
        val block2tx1 = block2tx.updated(cltvTimeout, block2tx.getOrElse(cltvTimeout, Seq.empty[Transaction]) :+ tx)
        context become watching(wallet, watches, watchedUtxos, block2tx1, None)
      } else publish(wallet, tx)

    case WatchEventConfirmed(BITCOIN_PARENT_TX_CONFIRMED(tx), blockHeight, _, _) =>
      log.info(s"parent tx of txid=${tx.txid} has been confirmed")
      val blockCount = this.blockCount.get()
      val csvTimeout = Scripts.csvTimeout(tx)
      val absTimeout = blockHeight + csvTimeout
      if (absTimeout > blockCount) {
        log.info(s"delaying publication of txid=${tx.txid} until block=$absTimeout (curblock=$blockCount)")
        val block2tx1 = block2tx.updated(absTimeout, block2tx.getOrElse(absTimeout, Seq.empty[Transaction]) :+ tx)
        context become watching(wallet, watches, watchedUtxos, block2tx1, None)
      } else publish(wallet, tx)

    case ValidateRequest(ann) => wallet.validate(ann).pipeTo(sender)

    case GetTxWithMeta(txid) => wallet.getTransactionMeta(txid).pipeTo(sender)

    case Terminated(channel) =>
      // we remove watches associated to dead actor
      val deprecatedWatches = watches.filter(_.channel == channel)
      val watchedUtxos1 = deprecatedWatches.foldLeft(watchedUtxos) { case (m, w) => removeWatchedUtxos(m, w) }
      context.become(watching(wallet, watches -- deprecatedWatches, watchedUtxos1, block2tx, None))

    case Symbol("watches") => sender ! watches
  }

  // NOTE: we use a single thread to publish transactions so that it preserves order.
  // CHANGING THIS WILL RESULT IN CONCURRENCY ISSUES WHILE PUBLISHING PARENT AND CHILD TXS
  val singleThreadExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def publish(wallet: BitcoinSWallet, tx: Transaction, isRetry: Boolean = false): Unit = {
    log.info(s"publishing tx (isRetry=$isRetry): txid=${tx.txid} tx=$tx")
    wallet.publishTransaction(tx)(singleThreadExecutionContext).recover {
      case t: Throwable if t.getMessage.contains("(code: -25)") && !isRetry => // we retry only once
        import akka.pattern.after

        import scala.concurrent.duration._
        after(3 seconds, context.system.scheduler)(Future.successful({})).map(_ => publish(wallet, tx, isRetry = true))
      case t: Throwable => log.error(s"cannot publish tx: reason=${t.getMessage} txid=${tx.txid} tx=$tx")
    }
  }

  def checkConfirmed(wallet: BitcoinSWallet, w: WatchConfirmed): Future[Unit] = {
    log.debug("checking confirmations of txid={}", w.txId)
    // NB: this is very inefficient since internally we call `getrawtransaction` three times, but it doesn't really
    // matter because this only happens once, when the watched transaction has reached min_depth
    wallet.getTxConfirmations(w.txId).flatMap {
      case Some(confirmations) if confirmations >= w.minDepth =>
        wallet.getTransaction(w.txId).flatMap { tx =>
          wallet.getTransactionShortId(w.txId).map {
            case (height, index) => self ! TriggerEvent(w, WatchEventConfirmed(w.event, height, index, tx))
          }
        }
    }
  }

}

object BitcoinSWatcher {
  /**
   *
   * @param txid funding transaction id
   * @return a (blockHeight, txIndex) tuple that is extracted from the input source
   *         This is used to create unique "dummy" short channel ids for zero-conf channels
   */
  def makeDummyShortChannelId(txid: ByteVector32): (Int, Int) = {
    // we use a height of 0
    // - to make sure that the tx will be marked as "confirmed"
    // - to easily identify scids linked to 0-conf channels
    //
    // this gives us a probability of collisions of 0.1% for 5 0-conf channels and 1% for 20
    // collisions mean that users may temporarily see incorrect numbers for their 0-conf channels (until they've been confirmed)
    // if this ever becomes a problem we could just extract some bits for our dummy height instead of just returning 0
    val height = 0
    val txIndex = txid.bits.sliceToInt(0, 16, false)
    (height, txIndex)
  }
}

