package fr.acinq.eclair.blockchain.bitcoins

import java.io.{ByteArrayInputStream, File}
import java.nio.file.{Files, Path, Paths}

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.bitcoin.{ByteVector32, Satoshi, Transaction}
import fr.acinq.eclair.blockchain.bitcoind.rpc._
import fr.acinq.eclair.blockchain.bitcoins.rpc.BitcoinSBitcoinClient
import fr.acinq.eclair.blockchain.{EclairWallet, GetTxWithMetaResponse, MakeFundingTxResponse, NewBlock, NewBlockHeader, NewTransaction, OnChainBalance, ValidateResult}
import fr.acinq.eclair.wire.ChannelAnnouncement
import grizzled.slf4j.Logging
import org.bitcoins.chain.blockchain.ChainHandler
import org.bitcoins.chain.config.ChainAppConfig
import org.bitcoins.chain.models.{BlockHeaderDAO, BlockHeaderDb, BlockHeaderDbHelper, CompactFilterDAO, CompactFilterHeaderDAO}
import org.bitcoins.chain.pow.Pow
import org.bitcoins.core.api.{ChainQueryApi, FeeRateApi, NodeApi}
import org.bitcoins.core.currency.Satoshis
import org.bitcoins.core.hd.AddressType
import org.bitcoins.core.protocol.blockchain.BlockHeader
import org.bitcoins.core.protocol.script.ScriptPubKey
import org.bitcoins.core.protocol.transaction.TransactionOutput
import org.bitcoins.core.util.{FutureUtil, NetworkUtil}
import org.bitcoins.core.wallet.fee._
import org.bitcoins.core.wallet.utxo.TxoState
import org.bitcoins.keymanager.bip39.{BIP39KeyManager, BIP39LockedKeyManager}
import org.bitcoins.node.{OnTxReceived, _}
import org.bitcoins.node.config.NodeAppConfig
import org.bitcoins.node.models.Peer
import org.bitcoins.wallet.Wallet
import org.bitcoins.wallet.api.WalletApi
import org.bitcoins.wallet.config.WalletAppConfig
import org.bitcoins.wallet.models.{AccountDAO, SpendingInfoDb}
import scodec.bits.ByteVector

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Properties
import scala.concurrent.duration._

class BitcoinSWallet(extendedBitcoinClient: BitcoinSBitcoinClient, watcher: Option[ActorRef], bip39PasswordOpt: Option[String] = None)(implicit system: ActorSystem,
                                                                                               walletConf: WalletAppConfig,
                                                                                               nodeConf: NodeAppConfig,
                                                                                               chainConf: ChainAppConfig) extends EclairWallet with Logging{

  logger.info("Initializing Bitcoin-S wallet")
  import system.dispatcher

  require(walletConf.defaultAddressType != AddressType.Legacy, "Must use segwit for LN")
  require(nodeConf.isNeutrinoEnabled, "Must use Neutrino for LN")

  def chainQueryApi: ChainQueryApi = wallet.chainQueryApi

  def nodeApi: NodeApi = wallet.nodeApi

  private val chainApiF = for {
    chainApi <- ChainHandler.fromDatabase(blockHeaderDAO = BlockHeaderDAO(),
      CompactFilterHeaderDAO(),
      CompactFilterDAO())
    isMissingChainWork <- chainApi.isMissingChainWork
    chainApiWithWork <-
      if (isMissingChainWork) {
        chainApi.recalculateChainWork
      } else {
        Future.successful(chainApi)
      }
  } yield chainApiWithWork

  private val peerSocket =
    NetworkUtil.parseInetSocketAddress(nodeConf.peers.head,
      nodeConf.network.port)
  private val peer = Peer.fromSocket(peerSocket)
  val uninitializedNodeF = chainApiF.flatMap { _ =>
    nodeConf.createNode(peer)(chainConf, system)
  }

  private val keyManager: BIP39KeyManager = {
    val kmParams = walletConf.kmParams
    val kmE = BIP39KeyManager.initialize(kmParams, None)
    kmE match {
      case Right(km) =>
        km
      case Left(err) =>
        sys.error(s"Could not read mnemonic=$err")
    }
  }

  private val feeRateApi: FeeRateApi = {
    new FeeRateApi {
      override def getFeeRate: Future[FeeUnit] = Future.successful(SatoshisPerVirtualByte(Satoshis.one))
    }
  }

  //get our wallet
  val configuredWalletF: Future[Wallet] = for {
    uninitializedNode <- uninitializedNodeF
    chainApi <- chainApiF
  } yield {
    Wallet(keyManager = keyManager,
      nodeApi = uninitializedNode,
      chainQueryApi = chainApi,
      creationTime = keyManager.creationTime,
      feeRateApi = feeRateApi
    )
  }

  val configuredNodeF: Future[Node] = for {
    uninitializedNode <- uninitializedNodeF
    wallet <- configuredWalletF
    callbacks <- createCallbacks(wallet)
  } yield {
    uninitializedNode.addCallbacks(callbacks)
  }

  val startedWalletF: Future[Wallet] = for {
    node <- configuredNodeF
    wallet <- configuredWalletF
    _ <- wallet.start()
    _ = watcher.foreach(_ ! this)
    _ <- node.start()
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

  override def getBalance: Future[OnChainBalance] = {
    for {
      confirmed <- wallet.getConfirmedBalance()
      unconfirmed <- wallet.getUnconfirmedBalance()
    } yield OnChainBalance(Satoshi(confirmed.satoshis.toLong), Satoshi(unconfirmed.satoshis.toLong))
  }

  override def getReceiveAddress: Future[String] = {
    wallet.getNewAddress().map(_.value)
  }

  override def getReceivePubkey(receiveAddress: Option[String]): Future[PublicKey] = Future {
    val xpub = keyManager.deriveXPub(walletConf.defaultAccount).get
    PublicKey(ByteVector.fromValidHex(xpub.key.hex))
  }

  override def makeFundingTx(pubkeyScript: ByteVector,
                             amount: Satoshi,
                             feeRatePerKw: Long): Future[MakeFundingTxResponse] = {
    val spk = ScriptPubKey.fromAsmBytes(pubkeyScript)
    val sats = Satoshis(amount.toLong)
    val output = Vector(TransactionOutput(sats, spk))
    val feeRate = SatoshisPerKW(Satoshis(feeRatePerKw))

    val fundedTxF = wallet.sendToOutputs(output, Some(feeRate), reserveUtxos = true)

    for {
      tx <- fundedTxF
      eclairTx = fr.acinq.bitcoin.Transaction.read(tx.bytes.toArray)
      outputIndex = tx.outputs.zipWithIndex
        .find(_._1.scriptPubKey == spk).get._2
      fee = feeRate.calc(tx)
    } yield MakeFundingTxResponse(eclairTx, outputIndex, Satoshi(fee.satoshis.toLong))
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
    extendedBitcoinClient.getTxConfirmations(txid)
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

  private def createCallbacks(wallet: WalletApi)(implicit
                                                 ec: ExecutionContext): Future[NodeCallbacks] = {
    lazy val onCompactFilters: OnCompactFiltersReceived = { blockFilters =>
      wallet
        .processCompactFilters(blockFilters = blockFilters)
        .map(_ => ())
    }
    lazy val onBlock: OnBlockReceived = { block =>
      for {
        _ <- wallet.processBlock(block).map(_ => ())
        _ <- wallet.updateUtxoPendingStates(block.blockHeader)
      } yield ()
    }
    lazy val onHeaders: OnBlockHeadersReceived = { headers =>
      if (headers.isEmpty) {
        Future.unit
      } else {
        for {
          _ <- wallet.updateUtxoPendingStates(headers.last)
          _ = watcher.foreach(_ ! NewBlockHeader(toAcinqBlockHeader(headers.last)))
        } yield ()
      }
    }
    lazy val onTxReceived: OnTxReceived = { tx =>
      Future.successful(watcher.foreach(_ ! NewTransaction(toAcinqTransaction(tx))))
    }
    Future.successful(
      NodeCallbacks(onBlockReceived = Vector(onBlock),
        onCompactFiltersReceived = Vector(onCompactFilters),
        onBlockHeadersReceived = Vector(onHeaders),
        onTxReceived = Vector(onTxReceived)
      ))
  }

  /** Creates a wallet based on the given [[WalletAppConfig]] */
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
//    for {
//      _ <- walletConf.initialize()
//      _ <- initWallet
//      callbacks <- createCallbacks(wallet)
//      _ = extendedBitcoind.addCallbacks(callbacks)
//    } yield {
//      this
//    }
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

  private def toAcinqTransaction(bitcoinSTx:  org.bitcoins.core.protocol.transaction.Transaction): fr.acinq.bitcoin.Transaction = {
    val acinqTx = fr.acinq.bitcoin.Transaction.read(toInputStream(bitcoinSTx.bytes))
    require(acinqTx.txid.bytes == bitcoinSTx.txId.bytes)
    acinqTx
  }
}

object BitcoinSWallet {
  val defaultDatadir: Path = Paths.get(Properties.userHome, ".bitcoin-s")

  def fromDatadir(extendedBitcoinClient: BitcoinSBitcoinClient,datadir: Path = defaultDatadir, watcher: Option[ActorRef] = None)(implicit system: ActorSystem): Future[BitcoinSWallet] = {
    import system.dispatcher
    val useLogback = true
    val segwitConf = ConfigFactory.parseString("bitcoin-s.wallet.defaultAccountType = segwit")
    val sysProps = ConfigFactory.parseProperties(System.getProperties)

    implicit val walletConf: WalletAppConfig = {
      val config = WalletAppConfig(datadir, useLogback, sysProps, segwitConf)
      config
    }

    implicit val nodeConf: NodeAppConfig = {
      val config = NodeAppConfig(datadir, useLogback, sysProps, segwitConf)
      config
    }

    implicit val chainConf: ChainAppConfig = {
      val config = ChainAppConfig(datadir, useLogback, sysProps, segwitConf)
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
