package fr.acinq.eclair.blockchain.bitcoins

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Path, Paths}

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.bitcoin.{ByteVector32, Satoshi, Transaction}
import fr.acinq.eclair.blockchain.EclairWallet.Neutrino
import fr.acinq.eclair.blockchain.bitcoind.BitcoinCoreWallet.WalletTransaction
import fr.acinq.eclair.blockchain.bitcoind.rpc._
import fr.acinq.eclair.blockchain.bitcoins.rpc.BitcoinSBitcoinClient
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.wire.ChannelAnnouncement
import grizzled.slf4j.Logging
import org.bitcoins.chain.blockchain.ChainHandler
import org.bitcoins.chain.config.ChainAppConfig
import org.bitcoins.chain.models._
import org.bitcoins.chain.pow.Pow
import org.bitcoins.core.api.node.NodeApi
import org.bitcoins.core.config.RegTest
import org.bitcoins.core.currency.Satoshis
import org.bitcoins.core.hd.AddressType
import org.bitcoins.core.protocol.BitcoinAddress
import org.bitcoins.core.protocol.blockchain.BlockHeader
import org.bitcoins.core.protocol.script.ScriptPubKey
import org.bitcoins.core.protocol.transaction.TransactionOutput
import org.bitcoins.core.util.{FutureUtil, NetworkUtil}
import org.bitcoins.core.wallet.fee._
import org.bitcoins.core.wallet.utxo.TxoState
import org.bitcoins.crypto.DoubleSha256DigestBE
import org.bitcoins.feeprovider.BitcoinerLiveFeeRateProvider
import org.bitcoins.keymanager.bip39.{BIP39KeyManager, BIP39LockedKeyManager}
import org.bitcoins.node.config.NodeAppConfig
import org.bitcoins.node.models.Peer
import org.bitcoins.node.{OnTxReceived, _}
import org.bitcoins.wallet.{OnTransactionProcessed, Wallet, WalletCallbacks}
import org.bitcoins.wallet.config.WalletAppConfig
import org.bitcoins.wallet.models.{AccountDAO, SpendingInfoDb}
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Properties

class BitcoinSWallet(extendedBitcoinClient: BitcoinSBitcoinClient, watcher: Option[ActorRef], bip39PasswordOpt: Option[String] = None)(implicit system: ActorSystem,
                                                                                                                                       walletConf: WalletAppConfig,
                                                                                                                                       nodeConf: NodeAppConfig,
                                                                                                                                       chainConf: ChainAppConfig) extends EclairWallet with Neutrino with Logging {
  logger.info("Initializing Bitcoin-S wallet")

  import system.dispatcher

  require(walletConf.defaultAddressType != AddressType.Legacy, "Must use segwit for LN")
  require(nodeConf.isNeutrinoEnabled, "Must use Neutrino for LN")

  def nodeApi: NodeApi = wallet.nodeApi

  private val chainApiF = for {
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
  val uninitializedNodeF = chainApiF.flatMap { _ =>
    nodeConf.createNode(peer)(chainConf, system)
  }

  //get our wallet
  val configuredWalletF: Future[Wallet] = for {
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

  val configuredNodeF: Future[Node] = for {
    uninitializedNode <- uninitializedNodeF
    wallet <- configuredWalletF
    nodeCallbacks <- createNodeCallbacks(wallet)
    _ = nodeConf.addCallbacks(nodeCallbacks)
  } yield {
    uninitializedNode
  }

  val startedWalletF: Future[Wallet] = for {
    node <- configuredNodeF
    wallet <- configuredWalletF
    _ <- node.start()
    _ <- wallet.start()
    _ = watcher.foreach(_ ! this)
    _ <- node.sync()
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
      txs.drop(skip).take(count).toList
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

  override def makeFundingTx(pubkeyScript: ByteVector,
                             amount: Satoshi,
                             feeRatePerKw: Long): Future[MakeFundingTxResponse] = {
    val spk = ScriptPubKey.fromAsmBytes(pubkeyScript)
    val sats = Satoshis(amount.toLong)
    val output = Vector(TransactionOutput(sats, spk))
    val feeRate = SatoshisPerKW(Satoshis(feeRatePerKw))

    val fundedTxF = wallet.sendToOutputs(output, feeRate)

    for {
      tx <- fundedTxF
      eclairTx = fr.acinq.bitcoin.Transaction.read(tx.bytes.toArray)
      outputIndex = tx.outputs.zipWithIndex
        .find(_._1.scriptPubKey == spk).get._2
      fee = feeRate.calc(tx)
    } yield MakeFundingTxResponse(eclairTx, outputIndex, Satoshi(fee.satoshis.toLong))
  }

  override def watchPubKeyScript(pubkeyScript: ByteVector): Future[Unit] = {
    val spk = ScriptPubKey.fromAsmBytes(pubkeyScript)
    wallet.watchScriptPubKey(spk).map(_ => ())
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

  def getTransaction(txid: ByteVector32): Future[Transaction] = {
    extendedBitcoinClient.getTransaction(txid)
  }

  def getTransactionMeta(txid: ByteVector32)(implicit ec: ExecutionContext): Future[GetTxWithMetaResponse] = {
    extendedBitcoinClient.getTransactionMeta(txid)
  }

  def getTxConfirmations(txid: ByteVector32): Future[Option[Int]] = {
    for {
      utxos <- listUtxos
      utxo = utxos.find(_.txid.bytes == txid.bytes)
      blockHash = for {
        u <- utxo
        b <- u.blockHash
      } yield b
      chainApi <- chainApiFromDb()
      conf <- blockHash match {
        case Some(b) =>
          chainApi.getNumberOfConfirmations(b)
        case _ => FutureUtil.none
      }
    } yield {
      conf
    }
  }

  def getTransactionShortId(txid: ByteVector32)(implicit ec: ExecutionContext): Future[(Int, Int)] =
    extendedBitcoinClient.getTransactionShortId(txid)

  def isTransactionOutputSpendable(txid: ByteVector32, outputIndex: Int, includeMempool: Boolean)(implicit ec: ExecutionContext): Future[Boolean] = {
    extendedBitcoinClient.isTransactionOutputSpendable(txid, outputIndex, includeMempool)
  }

  def getMempool()(implicit ec: ExecutionContext): Future[Seq[Transaction]] = {
    extendedBitcoinClient.getMempool()
  }

  def lookForSpendingTx(blockhash_opt: Option[ByteVector32], txid: ByteVector32, outputIndex: Int)(implicit ec: ExecutionContext): Future[Transaction] = {
    extendedBitcoinClient.lookForSpendingTx(blockhash_opt, txid, outputIndex)
  }

  def validate(c: ChannelAnnouncement)(implicit ec: ExecutionContext): Future[ValidateResult] = {
    extendedBitcoinClient.validate(c)
  }

  override def doubleSpent(tx: Transaction): Future[Boolean] = {
    // stolen from BitcoinCoreWallet.scala
    for {
      exists <- extendedBitcoinClient.getTransaction(tx.txid)
        .map(_ => true) // we have found the transaction
        .recover {
          case JsonRPCError(Error(_, message)) if message.contains("index") =>
            sys.error("Fatal error: bitcoind is indexing!!")
            System.exit(1) // bitcoind is indexing, that's a fatal error!!
            false // won't be reached
          case _ => false
        }
      doubleSpent <- if (exists) {
        // if the tx is in the blockchain, it can't have been double-spent
        Future.successful(false)
      } else {
        // if the tx wasn't in the blockchain and one of it's input has been spent, it is double-spent
        // NB: we don't look in the mempool, so it means that we will only consider that the tx has been double-spent if
        // the overriding transaction has been confirmed at least once
        Future.sequence(tx.txIn.map(txIn =>
          extendedBitcoinClient.isTransactionOutputSpendable(txIn.outPoint.txid, txIn.outPoint.index.toInt, includeMempool = false)))
          .map(_.exists(_ == false))
      }
    } yield doubleSpent
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

  def hasWallet: Future[Boolean] = {
    val walletDB = walletConf.dbPath resolve walletConf.dbName
    val hdCoin = walletConf.defaultAccount.coin
    if (Files.exists(walletDB) && walletConf.seedExists()) {
      AccountDAO().read((hdCoin, 0)).map(_.isDefined)
    } else {
      Future.successful(false)
    }
  }

  private def createWalletCallbacks()(implicit
                                      walletConf: WalletAppConfig,
                                      ec: ExecutionContext): Future[WalletCallbacks] = {
    lazy val onTransactionProcessed: OnTransactionProcessed = { tx =>
      Future.successful(watcher.foreach(_ ! NewTransaction(toAcinqTransaction(tx))))
    }

    Future.successful(WalletCallbacks(onTransactionProcessed = Vector(onTransactionProcessed)))
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
        for {
          _ <- wallet.updateUtxoPendingStates()
        } yield watcher.foreach(_ ! NewBlockHeader(toAcinqBlockHeader(headers.last)))
      }
    }
    Future.successful(
      NodeCallbacks(onBlockReceived = Vector(onBlock),
        onCompactFiltersReceived = Vector(onCompactFilters),
        onBlockHeadersReceived = Vector(onHeaders)
      ))
  }

  /** Creates a wallet based on the given [[WalletAppConfig]]*/
  def initWallet(implicit walletConf: WalletAppConfig, ec: ExecutionContext): Future[Wallet] = {
    hasWallet.flatMap { walletExists =>
      if (walletExists) {
        // TODO change me when we implement proper password handling
        BIP39LockedKeyManager.unlock(BIP39KeyManager.badPassphrase,
          bip39PasswordOpt,
          walletConf.kmParams) match {
          case Right(_) =>
            Future.successful(wallet)
          case Left(err) =>
            sys.error(s"Error initializing key manager, err=$err")
        }
      } else {
        Wallet.initialize(wallet = wallet,
          bip39PasswordOpt = bip39PasswordOpt)
      }
    }
  }

  val genesisHeader: BlockHeader = walletConf.network.chainParams.genesisBlock.blockHeader
  val genesisHeaderDb: BlockHeaderDb = BlockHeaderDbHelper.fromBlockHeader(height = 0, chainWork = Pow.getBlockProof(genesisHeader), bh = genesisHeader)

  /** Initializes the wallet and starts the node */
  def start(): Future[BitcoinSWallet] = {
    startedWalletF.map(_ => this)
  }

  private def toBitcoinsTx(tx: fr.acinq.bitcoin.Transaction): org.bitcoins.core.protocol.transaction.Transaction = {
    org.bitcoins.core.protocol.transaction.Transaction.fromBytes(tx.bin)
  }

  private def toInputStream(bytes: ByteVector): ByteArrayInputStream = new ByteArrayInputStream(bytes.toArray)

  private def toAcinqBlockHeader(bitcoinSHeader: org.bitcoins.core.protocol.blockchain.BlockHeader): fr.acinq.bitcoin.BlockHeader = {
    val acinqHeader = fr.acinq.bitcoin.BlockHeader.read(toInputStream(bitcoinSHeader.bytes))
    require(acinqHeader.hash.bytes == bitcoinSHeader.hash.bytes)
    acinqHeader
  }

  private def toAcinqTransaction(bitcoinSTx: org.bitcoins.core.protocol.transaction.Transaction): fr.acinq.bitcoin.Transaction = {
    val acinqTx = fr.acinq.bitcoin.Transaction.read(toInputStream(bitcoinSTx.bytes))
//    require(acinqTx.txid.bytes == bitcoinSTx.txIdBE.bytes)
    acinqTx
  }
}

object BitcoinSWallet {
  val defaultDatadir: Path = Paths.get(Properties.userHome, ".bitcoin-s")

  def fromDatadir(extendedBitcoinClient: BitcoinSBitcoinClient, datadir: Path = defaultDatadir, watcher: Option[ActorRef] = None, overrideConfig: Config = ConfigFactory.empty())(implicit system: ActorSystem): Future[BitcoinSWallet] = {
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
      _ <- chainConf.initialize()
      _ <- walletConf.initialize()
      _ <- nodeConf.initialize()
      wallet <- new BitcoinSWallet(extendedBitcoinClient, watcher).start()
    } yield wallet
  }
}
