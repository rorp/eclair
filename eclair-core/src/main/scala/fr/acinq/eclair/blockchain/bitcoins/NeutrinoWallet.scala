package fr.acinq.eclair.blockchain.bitcoins

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.bitcoin.{ByteVector32, Satoshi, Transaction}
import fr.acinq.eclair.blockchain.EclairWallet.Neutrino
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.blockchain.bitcoind.BitcoinCoreWallet.WalletTransaction
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import grizzled.slf4j.Logging
import org.bitcoins.chain.blockchain.ChainHandler
import org.bitcoins.chain.config.ChainAppConfig
import org.bitcoins.chain.models._
import org.bitcoins.chain.{ChainCallbacks, OnBlockHeaderConnected}
import org.bitcoins.core.api.wallet.db.SpendingInfoDb
import org.bitcoins.core.config.RegTest
import org.bitcoins.core.currency.Satoshis
import org.bitcoins.core.hd.AddressType
import org.bitcoins.core.protocol.BitcoinAddress
import org.bitcoins.core.protocol.script.ScriptPubKey
import org.bitcoins.core.protocol.transaction.TransactionOutput
import org.bitcoins.core.util.NetworkUtil
import org.bitcoins.core.wallet.fee._
import org.bitcoins.core.wallet.utxo.TxoState
import org.bitcoins.crypto.DoubleSha256DigestBE
import org.bitcoins.feeprovider.BitcoinerLiveFeeRateProvider
import org.bitcoins.node._
import org.bitcoins.node.config.NodeAppConfig
import org.bitcoins.node.models.Peer
import org.bitcoins.wallet.config.WalletAppConfig
import org.bitcoins.wallet.{OnBlockTransactionProcessed, OnTransactionProcessed, Wallet, WalletCallbacks}
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Properties

class NeutrinoWallet(watcher: Option[ActorRef], bip39PasswordOpt: Option[String] = None)(implicit system: ActorSystem,
                                                                                         walletConf: WalletAppConfig,
                                                                                         nodeConf: NodeAppConfig,
                                                                                         chainConf: ChainAppConfig) extends EclairWallet with Neutrino with Logging {
  logger.info("Initializing Bitcoin-S wallet")

  import system.dispatcher

  require(walletConf.defaultAddressType != AddressType.Legacy, "Must use segwit for LN")
  require(nodeConf.nodeType == NodeType.NeutrinoNode, "Must use Neutrino for LN")

  private val chainApiF = for {
    callbacks <- createChainCallbacks
    _ = chainConf.addCallbacks(callbacks)
    chainApi <- chainApiFromDb
    isMissingChainWork <- chainApi.isMissingChainWork
    chainApiWithWork <-
      if (isMissingChainWork) {
        chainApi.recalculateChainWork
      } else {
        Future.successful(chainApi)
      }
  } yield chainApiWithWork

  private def chainApiFromDb(): Future[ChainHandler] = {
    ChainHandler.fromDatabase(blockHeaderDAO = BlockHeaderDAO(),
      CompactFilterHeaderDAO(),
      CompactFilterDAO())
  }

  private val peerSocket =
    NetworkUtil.parseInetSocketAddress(nodeConf.peers.head,
      nodeConf.network.port)
  private val peer = Peer.fromSocket(peerSocket)
  private val uninitializedNodeF = chainApiF.flatMap { _ =>
    nodeConf.createNode(peer)(chainConf, system)
  }

  //get our wallet
  private val configuredWalletF: Future[Wallet] = for {
    uninitializedNode <- uninitializedNodeF
    chainApi <- chainApiF
    walletCallbacks <- createWalletCallbacks()
    _ = walletConf.addCallbacks(walletCallbacks)
    wallet <- walletConf.createHDWallet(uninitializedNode,
      chainApi,
      BitcoinerLiveFeeRateProvider(60),
      bip39PasswordOpt)
  } yield {
    wallet
  }

  private val configuredNodeF: Future[Node] = for {
    uninitializedNode <- uninitializedNodeF
    wallet <- configuredWalletF
    nodeCallbacks <- createNodeCallbacks(wallet)
    _ = nodeConf.addCallbacks(nodeCallbacks)
  } yield {
    uninitializedNode
  }

  private val startedWalletF: Future[Wallet] = for {
    node <- configuredNodeF
    wallet <- configuredWalletF
    _ <- node.start()
    _ <- wallet.start()
    _ = watcher.foreach(_ ! this)
    _ <- node.sync()
    //    _ <- wallet.rescanNeutrinoWallet(startOpt = None, endOpt = None, addressBatchSize = 20, useCreationTime = true)
  } yield {
    sys.addShutdownHook {
      wallet.stop()
      for {
        _ <- node.stop()
        _ = logger.info(s"Stopped neutrino node")
      } yield ()
    }
    wallet
  }

  startedWalletF.failed.foreach { err =>
    logger.error(s"Error on Bitcoin-S wallet startup!", err)
  }

  private lazy val wallet: Wallet = Await.result(startedWalletF, Duration.Inf)

  def getBlockHeight(blockHash: DoubleSha256DigestBE): Future[Option[Int]] =
    chainApiFromDb().flatMap(_.getBlockHeight(blockHash))

  def getBestBlockHash(): Future[DoubleSha256DigestBE] =
    chainApiFromDb().flatMap(_.getBestBlockHash())

  def getBestBlockHeader(): Future[(Int, fr.acinq.bitcoin.BlockHeader)] = {
    for {
      chainApi <- chainApiFromDb()
      headerDb <- chainApi.getBestBlockHeader()
    } yield {
      (headerDb.height, toAcinqBlockHeader(headerDb.blockHeader))
    }
  }

  def listTransactions(count: Int, skip: Int): Future[List[WalletTransaction]] = {
    for {
      utxos <- wallet.listUtxos()
    } yield {
      val txs = utxos.map { utxo =>
        WalletTransaction(
          address = BitcoinAddress.fromScriptPubKeyT(utxo.output.scriptPubKey, RegTest).get.toString,
          amount = Satoshi(utxo.output.value.satoshis.toLong),
          fees = Satoshi(0L),
          blockHash = utxo.blockHash.map(blockHash => ByteVector32(blockHash.bytes)).getOrElse(ByteVector32.Zeroes),
          confirmations = 0,
          txid = ByteVector32(utxo.txid.bytes),
          timestamp = 0
        )
      }
      txs.slice(skip, skip + count).toList
    }
  }

  def sendToAddress(address: String, amount: Satoshi, confirmationTarget: Long): Future[ByteVector32] =
    wallet.sendToAddress(BitcoinAddress.fromString(address), Satoshis.apply(amount.toLong), None).map(tx => ByteVector32(tx.txIdBE.bytes))

  override def getBalance: Future[OnChainBalance] = {
    for {
      confirmed <- wallet.getConfirmedBalance()
      unconfirmed <- wallet.getUnconfirmedBalance()
    } yield OnChainBalance(Satoshi(confirmed.satoshis.toLong), Satoshi(unconfirmed.satoshis.toLong))
  }

  override def getReceiveAddress: Future[String] = {
    wallet.getNewAddress().map(_.value)
  }

  override def getReceivePubkey(receiveAddress: Option[String] = None): Future[PublicKey] =
    for {
      addressStr <- receiveAddress.map(Future.successful).getOrElse(getReceiveAddress)
      address = BitcoinAddress.fromString(addressStr)
      addressInfo <- wallet.getAddressInfo(address)
    } yield {
      addressInfo
        .map(info => PublicKey(info.pubkey.bytes))
        .getOrElse(throw new RuntimeException(s"cannot get receive pubkey: unknown address $address"))
    }

  override def makeFundingTx(pubkeyScript: ByteVector, amount: Satoshi, feeRatePerKw: FeeratePerKw): Future[MakeFundingTxResponse] = {
    val spk = ScriptPubKey.fromAsmBytes(pubkeyScript)
    val sats = Satoshis(amount.toLong)
    val output = Vector(TransactionOutput(sats, spk))
    val feeRate = SatoshisPerKW(Satoshis(feeRatePerKw.feerate.toLong))

    val fundedTxF = wallet.sendToOutputs(output, feeRate)

    for {
      tx <- fundedTxF
      eclairTx = fr.acinq.bitcoin.Transaction.read(tx.bytes.toArray)
      outputIndex = tx.outputs.zipWithIndex
        .find(_._1.scriptPubKey == spk).get._2
      fee = feeRate.calc(tx)
    } yield MakeFundingTxResponse(eclairTx, outputIndex, Satoshi(fee.satoshis.toLong))
  }

  override def watchPublicKeyScript(pubkeyScript: ByteVector): Future[Unit] = {
    val spk = ScriptPubKey.fromAsmBytes(pubkeyScript)
    for {
      spks <- wallet.listScriptPubKeys()
      _ <- spks.find(_.scriptPubKey == spk) match {
        case None => wallet.watchScriptPubKey(spk)
        case Some(_) => Future.unit
      }
    } yield ()
  }

  override def commit(tx: Transaction): Future[Boolean] = {
    publishTransaction(tx).map(_ => true)
  }

  override def rollback(tx: Transaction): Future[Boolean] = {
    val bsTx = toBitcoinsTx(tx)
    val txOutPoints = bsTx.inputs.map(_.previousOutput)
    val utxosInTxF = wallet.listUtxos(txOutPoints.toVector)

    utxosInTxF.flatMap(utxos =>
      // fixme temp hack to pass invariant
      wallet.unmarkUTXOsAsReserved(utxos.map(_.copyWithState(TxoState.Reserved))))
      .map(_ => true)
  }

  override def doubleSpent(tx: Transaction): Future[Boolean] =
    for {
      txs <- wallet.listTransactions()
    } yield {
      txs
        .map(txn => toAcinqTransaction(txn.transaction))
        .exists { spendingTx =>
          spendingTx.txIn.map(_.outPoint).toSet.intersect(tx.txIn.map(_.outPoint).toSet).nonEmpty && spendingTx.txid != tx.txid
        }
    }

  def listReservedUtxos: Future[Vector[SpendingInfoDb]] = {
    wallet.listUtxos(TxoState.Reserved)
  }

  def listUtxos: Future[Vector[SpendingInfoDb]] = {
    wallet.listUtxos()
  }

  def publishTransaction(tx: Transaction)(implicit ec: ExecutionContext): Future[String] = {
    val txn = toBitcoinsTx(tx)
    for {
      _ <- wallet.processTransaction(txn, None)
      node <- configuredNodeF
      _ <- node.broadcastTransaction(txn)
    } yield tx.txid.toHex
  }

  private def createChainCallbacks()(implicit
                                     chainConf: ChainAppConfig,
                                     ec: ExecutionContext): Future[ChainCallbacks] = {
    lazy val onHeaderConnected: OnBlockHeaderConnected = {
      case (height, header) =>
        Future.successful(watcher.foreach(_ ! BlockHeaderConnected(height, toAcinqBlockHeader(header))))
    }

    Future.successful(ChainCallbacks(onBlockHeaderConnected = Vector(onHeaderConnected)))
  }

  private def createWalletCallbacks()(implicit
                                      walletConf: WalletAppConfig,
                                      ec: ExecutionContext): Future[WalletCallbacks] = {
    lazy val onBlockTransactionProcessed: OnBlockTransactionProcessed = {
      case (tx, blockHash, pos) =>
        for {
          height <- getBlockHeight(blockHash)
        } yield {
          watcher.foreach(_ ! TransactionProcessed(height.getOrElse(throw new RuntimeException(s"unknown block hash ${blockHash}")), toAcinqTransaction(tx), ByteVector32(blockHash.bytes), pos))
        }
    }

    Future.successful(WalletCallbacks(
      onBlockTransactionProcessed = Vector(onBlockTransactionProcessed)
    ))
  }

  private def createNodeCallbacks(wallet: Wallet)(implicit
                                                  nodeConf: NodeAppConfig,
                                                  ec: ExecutionContext): Future[NodeCallbacks] = {
    lazy val onCompactFilters: OnCompactFiltersReceived = { blockFilters =>
      wallet
        .processCompactFilters(blockFilters = blockFilters)
        .map(_ => ())
    }
    lazy val onBlock: OnBlockReceived = { block =>
      for {
        _ <- wallet.processBlock(block)
      } yield ()
    }
    lazy val onHeaders: OnBlockHeadersReceived = { headers =>
      if (headers.isEmpty) {
        Future.unit
      } else {
        wallet.updateUtxoPendingStates().map(_ => ())
      }
    }
    Future.successful(
      NodeCallbacks(onBlockReceived = Vector(onBlock),
        onCompactFiltersReceived = Vector(onCompactFilters),
        onBlockHeadersReceived = Vector(onHeaders)
      ))
  }

  /** Initializes the wallet and starts the node */
  def start(): Future[NeutrinoWallet] =
    startedWalletF.map(_ => this)

  private def toBitcoinsTx(tx: fr.acinq.bitcoin.Transaction): org.bitcoins.core.protocol.transaction.Transaction = {
    org.bitcoins.core.protocol.transaction.Transaction.fromBytes(tx.bin)
  }

  private def toInputStream(bytes: ByteVector): ByteArrayInputStream = new ByteArrayInputStream(bytes.toArray)

  private def toAcinqBlockHeader(bitcoinSHeader: org.bitcoins.core.protocol.blockchain.BlockHeader): fr.acinq.bitcoin.BlockHeader =
    fr.acinq.bitcoin.BlockHeader.read(toInputStream(bitcoinSHeader.bytes))

  private def toAcinqTransaction(bitcoinSTx: org.bitcoins.core.protocol.transaction.Transaction): fr.acinq.bitcoin.Transaction =
    fr.acinq.bitcoin.Transaction.read(toInputStream(bitcoinSTx.bytes))
}

object NeutrinoWallet {
  val defaultDatadir: Path = Paths.get(Properties.userHome, ".bitcoin-s")

  def fromDatadir(datadir: Path = defaultDatadir, watcher: Option[ActorRef] = None, overrideConfig: Config = ConfigFactory.empty())(implicit system: ActorSystem): Future[NeutrinoWallet] = {
    import system.dispatcher
    val segwitConf = ConfigFactory.parseString("bitcoin-s.wallet.defaultAccountType = segwit")
    val sysProps = ConfigFactory.parseProperties(System.getProperties)

    implicit val walletConf: WalletAppConfig = {
      val config = WalletAppConfig(datadir, sysProps, segwitConf, overrideConfig)
      config
    }

    implicit val nodeConf: NodeAppConfig = {
      val config = NodeAppConfig(datadir, sysProps, segwitConf, overrideConfig)
      config
    }

    implicit val chainConf: ChainAppConfig = {
      val config = ChainAppConfig(datadir, sysProps, segwitConf, overrideConfig)
      config
    }

    for {
      _ <- chainConf.start()
      _ <- walletConf.start()
      _ <- nodeConf.start()
      wallet <- new NeutrinoWallet(watcher).start()
    } yield wallet
  }
}
