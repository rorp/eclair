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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.pipe
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import fr.acinq.bitcoin.{Block, MilliBtc, Script, Transaction}
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService.BitcoinReq
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.transactions.Scripts
import fr.acinq.eclair.{LongToBtcAmount, TestKitBaseClass, addressToPublicKeyScript, randomKey}
import grizzled.slf4j.Logging
import org.bitcoins.core.api.wallet.db.SpendingInfoDb
import org.bitcoins.testkit.BitcoinSTestAppConfig
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JValue, _}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try


class NeutrinoWalletBasicSpec extends TestKitBaseClass with BitcoindService with AnyFunSuiteLike with BeforeAndAfterAll with Logging {

  val peerConfig = ConfigFactory.parseString(s"""bitcoin-s.node.peers = ["localhost:${bitcoindPort}"]""")

  val sender: TestProbe = TestProbe()
  val listener: TestProbe = TestProbe()

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def beforeAll(): Unit = {
    logger.info("starting bitcoind")
    startBitcoind()
    waitForBitcoindReady()
    generateBlocks(bitcoincli, 110)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    logger.info("stopping bitcoind")
    stopBitcoind()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  test("create/commit/rollback funding txes") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    fundWallet(wallet, 1)

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    assert(balance.confirmed + balance.unconfirmed > 0.sat)

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]
    assert(Try(addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash)).isSuccess)

    val fundingTxs = for (_ <- 0 to 3) yield {
      val pubkeyScript = Script.write(Script.pay2wsh(Scripts.multiSig2of2(randomKey.publicKey, randomKey.publicKey)))
      wallet.makeFundingTx(pubkeyScript, MilliBtc(50), FeeratePerKw(0 sat)).pipeTo(sender.ref) // create a tx with an invalid feerate (too little)
      val belowFeeFundingTx = sender.expectMsgType[MakeFundingTxResponse].fundingTx
      wallet.publishTransaction(belowFeeFundingTx).pipeTo(sender.ref) // try publishing the tx
      sender.expectMsgType[String]
      //      assert(sender.expectMsgType[Failure].cause.asInstanceOf[JsonRPCError].error.message.contains("min relay fee not met"))
      wallet.rollback(belowFeeFundingTx).pipeTo(sender.ref) // rollback the locked outputs
      assert(sender.expectMsgType[Boolean])

      // now fund a tx with correct feerate
      // fixme needed to change fee rate from 250 -> 251 because we round down fee rate vs core rounding up
      wallet.makeFundingTx(pubkeyScript, MilliBtc(50), FeeratePerKw(251 sat)).pipeTo(sender.ref)
      sender.expectMsgType[MakeFundingTxResponse].fundingTx
    }

    wallet.listUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 5)

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 0)

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

    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)
    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)
    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)
    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)
    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)
    generateBlocks(bitcoincli, 1, Some(addr))
    waitForNeutrinoSynced(wallet)

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 0)
  }

  test("unlock failed funding txes") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    fundWallet(wallet, 1)

    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    assert(balance.unconfirmed + balance.confirmed > 0.sat)

    wallet.getReceiveAddress.pipeTo(sender.ref)
    val address = sender.expectMsgType[String]
    assert(Try(addressToPublicKeyScript(address, Block.RegtestGenesisBlock.hash)).isSuccess)

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 0)

    val pubkeyScript = Script.write(Script.pay2wsh(Scripts.multiSig2of2(randomKey.publicKey, randomKey.publicKey)))
    wallet.makeFundingTx(pubkeyScript, MilliBtc(50), FeeratePerKw(10000 sat)).pipeTo(sender.ref)
    val fundingTx = sender.expectMsgType[MakeFundingTxResponse].fundingTx

    wallet.listUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 1)

    wallet.listReservedUtxos.pipeTo(sender.ref)
    assert(sender.expectMsgType[Vector[SpendingInfoDb]].size === 0)

    wallet.commit(fundingTx).pipeTo(sender.ref)
    assert(sender.expectMsgType[Boolean])

    wallet.getBalance.pipeTo(sender.ref)
    val balance1 = sender.expectMsgType[OnChainBalance]
    assert(balance1.unconfirmed + balance1.confirmed > 0.sat)
  }

  ignore("detect if tx has been double spent") {
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet
      .fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    waitForNeutrinoSynced(wallet)

    fundWallet(wallet, 1)

    // first let's create a tx
    val address = "n2YKngjUp139nkjKvZGnfLRN6HzzYxJsje"
    sender.send(bitcoincli, BitcoinReq("createrawtransaction", Array.empty, Map(address -> 6)))
    val JString(noinputTx1) = sender.expectMsgType[JString]
    sender.send(bitcoincli, BitcoinReq("fundrawtransaction", noinputTx1))
    val json = sender.expectMsgType[JValue]
    val JString(unsignedtx1) = json \ "hex"
    sender.send(bitcoincli, BitcoinReq("signrawtransactionwithwallet", unsignedtx1))
    val JString(signedTx1) = sender.expectMsgType[JValue] \ "hex"
    val tx1 = Transaction.read(signedTx1)

    // let's then generate another tx that double spends the first one
    val inputs = tx1.txIn.map(txIn => Map("txid" -> txIn.outPoint.txid.toString, "vout" -> txIn.outPoint.index)).toArray
    sender.send(bitcoincli, BitcoinReq("createrawtransaction", inputs, Map(address -> tx1.txOut.map(_.amount).sum.toLong * 1.0 / 1e8)))
    val JString(unsignedtx2) = sender.expectMsgType[JValue]
    sender.send(bitcoincli, BitcoinReq("signrawtransactionwithwallet", unsignedtx2))
    val JString(signedTx2) = sender.expectMsgType[JValue] \ "hex"
    val tx2 = Transaction.read(signedTx2)

    // test starts here

    // tx1/tx2 haven't been published, so tx1 isn't double spent
    wallet.doubleSpent(tx1).pipeTo(sender.ref)
    sender.expectMsg(false)
    // let's publish tx2
    sender.send(bitcoincli, BitcoinReq("sendrawtransaction", tx2.toString))
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

  def fundWallet(wallet: NeutrinoWallet, amountBtc: Double): Unit = {
    wallet.getBalance.pipeTo(sender.ref)
    val balance = sender.expectMsgType[OnChainBalance]
    logger.debug(s"initial balance: $balance")

    // send money to our wallet
    wallet.getReceiveAddress.pipeTo(sender.ref)
    val a = sender.expectMsgType[String]
    logger.debug(s"sending 2 btc to $a")
    sender.send(bitcoincli, BitcoinReq("sendtoaddress", a, amountBtc))
    val JString(txid) = sender.expectMsgType[JString]
    logger.debug(txid)

    awaitAssert({
      generateBlocks(bitcoincli, 1)
      waitForNeutrinoSynced(wallet)

      wallet.getBalance.pipeTo(sender.ref)
      val balance1 = sender.expectMsgType[OnChainBalance]
      logger.debug(s"cur balance: $balance1")
      assert(balance1.confirmed + balance1.unconfirmed == balance.confirmed + balance.unconfirmed + (amountBtc * 100000000).toInt.sat)
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
        bestHeight <- wallet.getFilterCount()
      } yield bestHeight).pipeTo(sender.ref)
      val walletBestHeight = sender.expectMsgType[Int]

      val networkBestHeight = getBestHeight(bitcoincli)

      logger.debug(s"$walletBestHeight $networkBestHeight")

      val res = walletBestHeight == networkBestHeight
      Thread.sleep(500)
      res
    }, max = 10 seconds, interval = 1 second)

  }

}
