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

import akka.actor.Status.Failure
import akka.pattern.pipe
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.{Block, MilliBtc, Satoshi, Script, Transaction}
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService
import fr.acinq.eclair.blockchain.bitcoind.rpc.{BasicBitcoinJsonRPCClient, JsonRPCError}
import fr.acinq.eclair.blockchain.bitcoins.rpc.BitcoinSBitcoinClient
import fr.acinq.eclair.transactions.Scripts
import fr.acinq.eclair.{LongToBtcAmount, TestKitBaseClass, addressToPublicKeyScript, randomKey}
import grizzled.slf4j.Logging
import org.bitcoins.core.api.wallet.db.SpendingInfoDb
import org.bitcoins.core.protocol.BitcoinAddress
import org.bitcoins.crypto.DoubleSha256DigestBE
import org.bitcoins.testkit.BitcoinSTestAppConfig
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try


class BitcoinSWalletBasicSpec extends TestKitBaseClass with BitcoindService with AnyFunSuiteLike with BeforeAndAfterAll with Logging {

  val commonConfig: Config = ConfigFactory.parseMap(Map(
    "eclair.chain" -> "regtest",
    "eclair.spv" -> false,
    "eclair.server.public-ips.1" -> "localhost",
    "eclair.bitcoind.port" -> bitcoindPort,
    "eclair.bitcoind.rpcport" -> bitcoindRpcPort,
    "eclair.router-broadcast-interval" -> "2 second",
    "eclair.auto-reconnect" -> false).asJava)
  val config: Config = ConfigFactory.load(commonConfig).getConfig("eclair")

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def beforeAll(): Unit = {
    startBitcoind()
  }

  override def afterAll: Unit = {
    stopBitcoind()
  }

  def initWallet: Future[(BitcoinSWallet, BitcoinSBitcoinClient)] = {
    val bitcoinClient = new BasicBitcoinJsonRPCClient(
      user = config.getString("bitcoind.rpcuser"),
      password = config.getString("bitcoind.rpcpassword"),
      host = config.getString("bitcoind.host"),
      port = config.getInt("bitcoind.rpcport"))
    val extendedBitcoind = new BitcoinSBitcoinClient(bitcoinClient)

    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    for {
      wallet <- BitcoinSWallet
        .fromDatadir(extendedBitcoind, datadir)
      started <- wallet.start()
      addr <- started.getReceiveAddress
      // fixme kinda hacky, but this way we only process confirmed blocks and don't spend immature coinbases
      hashes <- extendedBitcoind.generateToAddress(10, BitcoinAddress.fromString(addr))
      _ <- extendedBitcoind.generateToAddress(101, BitcoinAddress.fromString(addr))
      _ <- extendedBitcoind.downloadBlocks(hashes.map(_.flip))
    } yield (started, extendedBitcoind)
  }

  test("wait bitcoind ready") {
    waitForBitcoindReady()
  }

  test("create/commit/rollback funding txes") {

    val sender = TestProbe()

    initWallet.pipeTo(sender.ref)
    val (wallet, extendedBitcoind) = sender.expectMsgType[(BitcoinSWallet, BitcoinSBitcoinClient)]

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[Satoshi] > 0.sat)
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]
    assert(Try(addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash)).isSuccess)

    val fundingTxs = for (_ <- 0 to 3) yield {
      val pubkeyScript = Script.write(Script.pay2wsh(Scripts.multiSig2of2(randomKey.publicKey, randomKey.publicKey)))
      wallet.makeFundingTx(pubkeyScript, MilliBtc(50), 200).pipeTo(sender.ref) // create a tx with an invalid feerate (too little)
      val belowFeeFundingTx = sender.expectMsgType[MakeFundingTxResponse].fundingTx
      wallet.publishTransaction(belowFeeFundingTx).pipeTo(sender.ref) // try publishing the tx
      assert(sender.expectMsgType[Failure].cause.asInstanceOf[JsonRPCError].error.message.contains("min relay fee not met"))
      wallet.rollback(belowFeeFundingTx).pipeTo(sender.ref) // rollback the locked outputs
      assert(sender.expectMsgType[Boolean])

      // now fund a tx with correct feerate
      // fixme needed to change fee rate from 250 -> 251 because we round down fee rate vs core rounding up
      wallet.makeFundingTx(pubkeyScript, MilliBtc(50), 251).pipeTo(sender.ref)
      sender.expectMsgType[MakeFundingTxResponse].fundingTx
    }

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]](10 seconds).size === 4)

    wallet.commit(fundingTxs(0)).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.rollback(fundingTxs(1)).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.commit(fundingTxs(2)).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.rollback(fundingTxs(3)).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val addr = sender.expectMsgType[String]

    extendedBitcoind.generateToAddress(6, BitcoinAddress.fromString(addr)).pipeTo(sender.ref)
    val hashes = sender.expectMsgType[Vector[DoubleSha256DigestBE]]

    extendedBitcoind.downloadBlocks(hashes.map(_.flip)).pipeTo(sender.ref)
    sender.expectMsgType[Unit]

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]](10 seconds).size === 0)
  }

  test("unlock failed funding txes") {

    val sender = TestProbe()

    initWallet.pipeTo(sender.ref)
    val wallet = sender.expectMsgType[BitcoinSWallet]

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[Satoshi] > 0.sat)

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]
    assert(Try(addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash)).isSuccess)

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]](10 seconds).size === 0)

    val pubkeyScript = Script.write(Script.pay2wsh(Scripts.multiSig2of2(randomKey.publicKey, randomKey.publicKey)))
    wallet.makeFundingTx(pubkeyScript, MilliBtc(50), 10000).pipeTo(sender.ref)
    val fundingTx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]](10 seconds).size === 1)

    wallet.commit(fundingTx).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.getBalance.pipeTo(sender.ref)
    assert(sender.expectMsgType[Satoshi] > 0.sat)
  }

  test("detect if tx has been double spent") {
    val bitcoinClient = new BasicBitcoinJsonRPCClient(
      user = config.getString("bitcoind.rpcuser"),
      password = config.getString("bitcoind.rpcpassword"),
      host = config.getString("bitcoind.host"),
      port = config.getInt("bitcoind.rpcport"))
    val extendedBitcoind = new BitcoinSBitcoinClient(bitcoinClient)
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()

    val walletF: Future[BitcoinSWallet] = {
      for {
        wallet <- BitcoinSWallet
          .fromDatadir(extendedBitcoind, datadir)
        started <- wallet.start()
        addr <- started.getReceiveAddress
        // fixme kinda hacky, but this way we only process confirmed blocks and don't spend immature coinbases
        hashes <- extendedBitcoind.generateToAddress(10, BitcoinAddress.fromString(addr))
        _ <- extendedBitcoind.generateToAddress(101, BitcoinAddress.fromString(addr))
        _ <- extendedBitcoind.downloadBlocks(hashes.map(_.flip))
      } yield started
    }

    val sender = TestProbe()

    walletF.pipeTo(sender.ref)
    val wallet = sender.expectMsgType[BitcoinSWallet]

    // first let's create a tx
    val address = "n2YKngjUp139nkjKvZGnfLRN6HzzYxJsje"
    bitcoinClient.invoke("createrawtransaction", Array.empty, Map(address -> 6)).pipeTo(sender.ref)
    val JString(noinputTx1) = sender.expectMsgType[JString]
    bitcoinClient.invoke("fundrawtransaction", noinputTx1).pipeTo(sender.ref)
    val json = sender.expectMsgType[JValue]
    val JString(unsignedtx1) = json \ "hex"
    bitcoinClient.invoke("signrawtransactionwithwallet", unsignedtx1).pipeTo(sender.ref)
    val JString(signedTx1) = sender.expectMsgType[JValue] \ "hex"
    val tx1 = Transaction.read(signedTx1)
    // let's then generate another tx that double spends the first one
    val inputs = tx1.txIn.map(txIn => Map("txid" -> txIn.outPoint.txid.toString, "vout" -> txIn.outPoint.index)).toArray
    bitcoinClient.invoke("createrawtransaction", inputs, Map(address -> tx1.txOut.map(_.amount).sum.toLong * 1.0 / 1e8)).pipeTo(sender.ref)
    val JString(unsignedtx2) = sender.expectMsgType[JValue]
    bitcoinClient.invoke("signrawtransactionwithwallet", unsignedtx2).pipeTo(sender.ref)
    val JString(signedTx2) = sender.expectMsgType[JValue] \ "hex"
    val tx2 = Transaction.read(signedTx2)

    // test starts here

    // tx1/tx2 haven't been published, so tx1 isn't double spent
    wallet.doubleSpent(tx1).pipeTo(sender.ref)
    sender.expectMsg(false)
    // let's publish tx2
    bitcoinClient.invoke("sendrawtransaction", tx2.toString).pipeTo(sender.ref)
    val JString(_) = sender.expectMsgType[JValue]
    // tx2 hasn't been confirmed so tx1 is still not considered double-spent
    wallet.doubleSpent(tx1).pipeTo(sender.ref)
    sender.expectMsg(false)
    // let's confirm tx2
    generateBlocks(bitcoincli, 1)
    // this time tx1 has been double spent
    wallet.doubleSpent(tx1).pipeTo(sender.ref)
    sender.expectMsg(true)
  }
}
