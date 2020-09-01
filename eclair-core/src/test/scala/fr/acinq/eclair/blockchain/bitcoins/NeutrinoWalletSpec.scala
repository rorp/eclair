/*
 * Copyright 2019 ACINQ SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.acinq.eclair.blockchain.bitcoins

import java.nio.file.Path

import akka.pattern.pipe
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.{Block, Satoshi}
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService
import fr.acinq.eclair.blockchain.bitcoind.rpc.BasicBitcoinJsonRPCClient
import fr.acinq.eclair.blockchain.bitcoins.rpc.BitcoinSBitcoinClient
import fr.acinq.eclair.{LongToBtcAmount, TestKitBaseClass}
import grizzled.slf4j.Logging
import org.bitcoins.core.protocol.BitcoinAddress
import org.bitcoins.crypto.DoubleSha256DigestBE
import org.bitcoins.testkit.BitcoinSTestAppConfig
import org.json4s.DefaultFormats
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._


class NeutrinoWalletSpec extends TestKitBaseClass with BitcoindService with AnyFunSuiteLike with BeforeAndAfterAll with Logging {

  val commonConfig: Config = ConfigFactory.parseMap(Map(
    "eclair.chain" -> "regtest",
    "eclair.spv" -> false,
    "eclair.server.public-ips.1" -> "localhost",
    "eclair.bitcoind.port" -> bitcoindPort,
    "eclair.bitcoind.rpcport" -> bitcoindRpcPort,
    "eclair.router-broadcast-interval" -> "2 second",
    "eclair.auto-reconnect" -> false).asJava)
  val config: Config = ConfigFactory.load(commonConfig).getConfig("eclair")

  val sender: TestProbe = TestProbe()
  val listener: TestProbe = TestProbe()

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def beforeAll(): Unit = {
    startBitcoind()
  }

  override def afterAll(): Unit = {
    stopBitcoind()
  }

  def initWallet: Future[(NeutrinoWallet, BitcoinSBitcoinClient)] = {
    val bitcoinClient = new BasicBitcoinJsonRPCClient(
      user = config.getString("bitcoind.rpcuser"),
      password = config.getString("bitcoind.rpcpassword"),
      host = config.getString("bitcoind.host"),
      port = config.getInt("bitcoind.rpcport"))
    val extendedBitcoind = new BitcoinSBitcoinClient(bitcoinClient)

    val peerConfig = ConfigFactory.parseString(s"""bitcoin-s.node.peers = ["${config.getString("bitcoind.host")}:${config.getInt("bitcoind.port")}"]""")

    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    Future.successful((wallet, extendedBitcoind))
  }

  def initFundedWallet: Future[NeutrinoWallet] = {
    for {
      (wallet, extendedBitcoind) <- initWallet
      addr <- wallet.getReceiveAddress
      // fixme kinda hacky, but this way we only process confirmed blocks and don't spend immature coinbases
      hashes <- extendedBitcoind.generateToAddress(10, BitcoinAddress.fromString(addr))
      _ <- extendedBitcoind.generateToAddress(101, BitcoinAddress.fromString(addr))
      _ <- extendedBitcoind.downloadBlocks(hashes.map(_.flip))
    } yield wallet
  }

  test("wait bitcoind ready") {
    waitForBitcoindReady()
  }

  test("process a block") {
    initWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(NeutrinoWallet, BitcoinSBitcoinClient)]

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[Satoshi] == 0.sat)

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    extendedBitcoind.generateToAddress(101, address).pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]

    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[Satoshi] > 0.sat)
  }
  /*

  test("receive funds") {
    initWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(BitcoinSWallet, BitcoinSBitcoinClient)]

    // fund bitcoind
    val bitcoindAddressF = extendedBitcoind.rpcClient.invoke("getnewaddress") collect { case JString(str) => str }
    bitcoindAddressF.pipeTo(sender.ref)
    val bitcoindAddress = sender.expectMsgType[String]
    extendedBitcoind.generateToAddress(numBlocks = 101, bitcoindAddress).pipeTo(sender.ref)
    sender.expectMsgType[Vector[DoubleSha256DigestBE]]

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[Satoshi]
    logger.info(s"initial balance: $balance")

    // send money to our wallet
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    logger.info(s"sending 1 btc to $address")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address, 1.0))
    sender.expectMsgType[JValue]

    extendedBitcoind.generateToAddress(numBlocks = 1, bitcoindAddress).pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]
    assert(hashes.size == 1)
    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    awaitCond({
      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[Satoshi]

      wallet.listUtxos.pipeTo(sender.ref)
      val utxos = sender.expectMsgType[Vector[SpendingInfoDb]]

      balance1 == balance + 100000000.sat
    }, max = 10 seconds, interval = 1 second)

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address1 = sender.expectMsgType[String]

    logger.info(s"sending 1 btc to $address1")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address1, 1.0))
    sender.expectMsgType[JValue]
    logger.info(s"sending 0.5 btc to $address1")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", address1, 0.5))
    sender.expectMsgType[JValue]

    extendedBitcoind.generateToAddress(101, bitcoindAddress).pipeTo(sender.ref)
    val hashes1 = sender.expectMsgType[Vector[DoubleSha256DigestBE]]
    extendedBitcoind.downloadBlocks(hashes1.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    awaitCond({
      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[Satoshi]
      balance1 == balance + 250000000.sat
    }, max = 10 seconds, interval = 1 second)
  }


  test("handle transactions with identical outputs to us") {
    initWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(BitcoinSWallet, BitcoinSBitcoinClient)]

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[Satoshi]
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
      FundTransactionResponse(tx1, _, _) <- btcWallet.fundTransaction(tx, false, 10000)
      SignTransactionResponse(tx2, true) <- btcWallet.signTransaction(tx1)
      txid <- btcWallet.commit(tx2)
    } yield txid
    Await.result(future, 10 seconds)

    // gen to junk address
    extendedBitcoind.generateToAddress(numBlocks = 1, "2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U").pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]
    assert(hashes.size == 1)
    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    awaitCond({
      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[Satoshi]
      balance1 == balance + amount + amount
    }, max = 30 seconds, interval = 1 second)
  }


  test("send money to someone else (we broadcast)") {
    initFundedWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(BitcoinSWallet, BitcoinSBitcoinClient)]

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[Satoshi]
    logger.info(s"initial balance: $balance")

    // create a tx that sends money to Bitcoin Core's address
    sender.send(bitcoincli, BitcoinReq("getnewaddress"))
    val JString(address) = sender.expectMsgType[JValue]
    val addr = BitcoinAddress.fromString(address)
    wallet.makeFundingTx(addr.scriptPubKey.asmBytes, Btc(1).toSatoshi, 350).pipeTo(sender.ref)
    val tx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    // send it ourselves
    logger.info(s"sending 1 btc to $address with tx ${tx.txid}")
    wallet.publishTransaction(tx).pipeTo(sender.ref)
    sender.expectMsgType[String]

    extendedBitcoind.generateToAddress(numBlocks = 1, "2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U").pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]
    assert(hashes.size == 1)
    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    awaitCond({
      sender.send(bitcoincli, BitcoinReq("getreceivedbyaddress", address))
      val JDecimal(value) = sender.expectMsgType[JValue]
      value == BigDecimal(1.0)
    }, max = 30 seconds, interval = 1 second)

    awaitCond({
      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[Satoshi]
      logger.debug(s"current balance is $balance1")
      balance1 < balance - 1.btc && balance1 > balance - 1.btc - 50000.sat
    }, max = 10 seconds, interval = 1 second)
  }


  test("send money to ourselves (we broadcast)") {
    initFundedWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(BitcoinSWallet, BitcoinSBitcoinClient)]

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[Satoshi]
    logger.info(s"initial balance: $balance")

    // create a tx that sends money to Bitcoin Core's address
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]

    val addr = BitcoinAddress.fromString(address)
    wallet.makeFundingTx(addr.scriptPubKey.asmBytes, Btc(1).toSatoshi, 350).pipeTo(sender.ref)
    val tx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    // send it ourselves
    logger.info(s"sending 1 btc to $address with tx ${tx.txid}")
    wallet.publishTransaction(tx).pipeTo(sender.ref)
    sender.expectMsgType[String]

    extendedBitcoind.generateToAddress(numBlocks = 1, "2NFyxovf6MyxfHqtVjstGzs6HeLqv92Nq4U").pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]
    assert(hashes.size == 1)
    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    awaitCond({
      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[Satoshi]
      logger.debug(s"current balance is $balance1")
      balance1 < balance && balance1 > balance - 50000.sat
    }, max = 10 seconds, interval = 1 second)
  }

   */
}