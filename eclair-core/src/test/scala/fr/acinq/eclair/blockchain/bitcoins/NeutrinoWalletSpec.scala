package fr.acinq.eclair.blockchain.bitcoins

import java.nio.file.Path

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.pipe
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import fr.acinq.bitcoin.{Block, Btc, Satoshi, Transaction, TxOut}
import fr.acinq.eclair.blockchain.bitcoind.BitcoinCoreWallet.{FundTransactionResponse, SignTransactionResponse}
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService.BitcoinReq
import fr.acinq.eclair.blockchain.bitcoind.{BitcoinCoreWallet, BitcoindService}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.blockchain.{MakeFundingTxResponse, OnChainBalance}
import fr.acinq.eclair.{LongToBtcAmount, TestKitBaseClass}
import grizzled.slf4j.Logging
import org.bitcoins.core.protocol.BitcoinAddress
import org.bitcoins.testkit.BitcoinSTestAppConfig
import org.json4s.JsonAST.{JDecimal, JInt, JObject, JString}
import org.json4s.{DefaultFormats, JValue}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DurationInt, FiniteDuration}


class NeutrinoWalletSpec extends TestKitBaseClass with BitcoindService with AnyFunSuiteLike with BeforeAndAfterAll with Logging {

  val peerConfig = ConfigFactory.parseString(s"""bitcoin-s.node.peers = ["localhost:${bitcoindPort}"]""")

  val sender: TestProbe = TestProbe()
  val listener: TestProbe = TestProbe()

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def beforeAll(): Unit = {
    logger.info("starting bitcoind")
    startBitcoind()
    waitForBitcoindReady()
    generateBlocks(bitcoincli, 101)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    logger.info("stopping bitcoind")
    stopBitcoind()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  test("process a block") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[OnChainBalance] == OnChainBalance(0 sat, 0 sat))

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    generateBlocks(bitcoincli, 100, Some(address))

    awaitAssert({
      generateBlocks(bitcoincli, 1, Some(address))
      wallet.getBalance.pipeTo(sender.ref)
      val balance = sender.expectMsgType[OnChainBalance]
      assert(balance.confirmed + balance.unconfirmed > 0.sat)
    }, max = 10 seconds, interval = 1 second)
  }


  test("receive funds") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    logger.debug(s"initial balance: $balance")

    // send money to our wallet
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    logger.debug(s"sending 1 btc to $address")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address, 1.0))
    sender.expectMsgType[JValue]

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address1 = sender.expectMsgType[String]

    logger.debug(s"sending 1 btc to $address1")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address1, 1.0))
    sender.expectMsgType[JValue]
    logger.debug(s"sending 0.5 btc to $address1")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address1, 0.5))
    sender.expectMsgType[JValue]

    awaitAssert({
      generateBlocks(bitcoincli, 1)
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      logger.debug(balance1)
      assert(balance1.confirmed + balance1.unconfirmed == balance.confirmed + balance.unconfirmed + 250000000.sat)
    }, max = 10 seconds, interval = 1 second)
  }

  test("handle transactions with identical outputs to us") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    logger.info(s"initial balance: $balance")

    // send money to our wallet
    val amount = 750000.sat
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    val tx = Transaction(version = 2,
      txIn = Nil,
      txOut = Seq(
        TxOut(amount, fr.acinq.eclair.addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash)),
        TxOut(amount, fr.acinq.eclair.addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash))
      ), lockTime = 0L)

    val btcWallet = new BitcoinCoreWallet(bitcoinrpcclient)
    val future = for {
      FundTransactionResponse(tx1, _, _) <- btcWallet.fundTransaction(tx, false, FeeratePerKw(Satoshi(10000)))
      SignTransactionResponse(tx2, true) <- btcWallet.signTransaction(tx1)
      res <- btcWallet.commit(tx2)
    } yield res

    future.pipeTo(sender.ref)
    sender.expectMsgType[Boolean]

    awaitAssert({
      // gen to junk address
      generateBlocks(bitcoincli, 1, Some("2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U"))
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      assert(balance1.unconfirmed + balance1.confirmed == balance.unconfirmed + balance.confirmed + amount + amount)
    }, max = 30 seconds, interval = 1 second)
  }

  test("send money to someone else (we broadcast)") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    sender.send(bitcoincli, BitcoinReq("getbalance"))
    val jv = sender.expectMsgType[JDecimal]
    println(jv)

    // send money to our wallet
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val a = sender.expectMsgType[String]
    logger.debug(s"sending 2 btc to $a")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", a, 2.0))
    val js = sender.expectMsgType[JValue]
    println(js)

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    println(s"initial balance: $balance")

    awaitAssert({
      generateBlocks(bitcoincli, 1)
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      println(s"cur balance: $balance1")
      assert(balance1.confirmed + balance1.unconfirmed == balance.confirmed + balance.unconfirmed + 100000000.sat)
    }, max = 10 seconds, interval = 1 second)


    // create a tx that sends money to Bitcoin Core's address
    sender.send(bitcoincli, BitcoinReq("getnewaddress"))
    val JString(address) = sender.expectMsgType[JValue]
    val addr = BitcoinAddress.fromString(address)
    wallet.makeFundingTx(addr.scriptPubKey.asmBytes, Btc(1).toSatoshi, FeeratePerKw(Satoshi(350))).pipeTo(sender.ref)
    val tx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    // send it ourselves
    logger.debug(s"sending 1 btc to $address with tx ${tx.txid}")
    wallet.publishTransaction(tx).pipeTo(sender.ref)
    sender.expectMsgType[String]
    logger.debug(tx)

    awaitAssert({
      generateBlocks(bitcoincli, 1, Some("2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U"))
      waitForNeutrinoSynced(wallet)

      sender.send(bitcoincli, BitcoinReq("getreceivedbyaddress", address))
      val JDecimal(value) = sender.expectMsgType[JValue]
      logger.debug(value)
      assert(value == BigDecimal(1.0))
    }, max = 30 seconds, interval = 1 second)

    awaitAssert({
      generateBlocks(bitcoincli, 1, Some("2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U"))
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      logger.debug(s"current balance is $balance1")
      assert(balance1.confirmed + balance1.unconfirmed < balance.confirmed + balance.unconfirmed - 1.btc && balance1.confirmed + balance1.unconfirmed > balance.confirmed + balance.unconfirmed - 1.btc - 50000.sat)
    }, max = 10 seconds, interval = 1 second)
  }


  test("send money to ourselves (we broadcast)") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    sender.send(bitcoincli, BitcoinReq("getbalance"))
    val JDecimal(bal) = sender.expectMsgType[JDecimal]
    logger.debug(s"network balance: $bal")

    // send money to our wallet
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val a = sender.expectMsgType[String]
    logger.debug(s"sending 2 btc to $a")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", a, 2.0))
    val JString(txid) = sender.expectMsgType[JString]
    logger.debug(txid)

    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)

    waitForNeutrinoSynced(wallet)


    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    logger.debug(s"initial balance: $balance")

    // create a tx that sends money to Bitcoin Core's address
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    val addr = BitcoinAddress.fromString(address)
    wallet.makeFundingTx(addr.scriptPubKey.asmBytes, Btc(1).toSatoshi, FeeratePerKw(Satoshi(350))).pipeTo(sender.ref)
    val tx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    // send it ourselves
    logger.info(s"sending 1 btc to $address with tx ${tx.txid}")
    wallet.publishTransaction(tx).pipeTo(sender.ref)
    sender.expectMsgType[String]

    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)
    generateBlocks(bitcoincli, 1)

    awaitCond({
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      logger.debug(s"current balance is $balance1")
      balance1.confirmed + balance1.unconfirmed < balance.confirmed + balance.unconfirmed && balance1.confirmed + balance1.unconfirmed > balance.confirmed + balance.unconfirmed - 50000.sat
    }, max = 10 seconds, interval = 1 second)
  }

  def getBestHeight(bitcoinCli: ActorRef, timeout: FiniteDuration = 10 seconds)(implicit system: ActorSystem): Int = {
    val sender = TestProbe()
    sender.send(bitcoinCli, BitcoinReq("getbestblockhash"))
    val JString(blockHash) = sender.expectMsgType[JValue](timeout)
    sender.send(bitcoinCli, BitcoinReq("getblock", blockHash))
    val JObject(block) = sender.expectMsgType[JValue](timeout)
    val height_opt = block.collectFirst { case ("height", JInt(height)) => height }
    assert(height_opt.nonEmpty)
    height_opt.get.toInt
  }

  def waitForNeutrinoSynced(wallet: NeutrinoWallet): Unit = {
    awaitCond({
      (for {
        bestHash <- wallet.getBestBlockHash()
        bestHeight <- wallet.getBlockHeight(bestHash)
      } yield bestHeight).pipeTo(sender.ref)
      val walletBestHeight = sender.expectMsgType[Option[Int]].get

      val networkBestHeight = getBestHeight(bitcoincli)

      println(s"$walletBestHeight $networkBestHeight")

      walletBestHeight == networkBestHeight
    }, max = 10 seconds, interval = 1 second)

  }
}