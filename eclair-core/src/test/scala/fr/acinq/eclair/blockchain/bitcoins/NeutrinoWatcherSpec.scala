package fr.acinq.eclair.blockchain.bitcoins

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Props
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import fr.acinq.bitcoin.{Block, OutPoint, SIGHASH_ALL, Script, ScriptFlags, ScriptWitness, SigVersion, Transaction, TxIn, TxOut}
import fr.acinq.eclair.blockchain.WatcherSpec._
import fr.acinq.eclair.blockchain._
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService.BitcoinReq
import fr.acinq.eclair.channel.{BITCOIN_FUNDING_DEPTHOK, BITCOIN_FUNDING_SPENT}
import fr.acinq.eclair.{LongToBtcAmount, TestKitBaseClass}
import grizzled.slf4j.Logging
import org.bitcoins.testkit.BitcoinSTestAppConfig
import org.json4s.JsonAST.JValue
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.duration._

class NeutrinoWatcherSpec extends TestKitBaseClass with AnyFunSuiteLike with BitcoindService with BeforeAndAfterAll with Logging {

  val peerConfig = ConfigFactory.parseString(s"""bitcoin-s.node.peers = ["localhost:${bitcoindPort}"]""")

  override def beforeAll(): Unit = {
    logger.info("starting bitcoind")
    startBitcoind()
    waitForBitcoindReady()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    logger.info("stopping bitcoind")
    stopBitcoind()
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  test("watch for confirmed transactions") {
    val probe = TestProbe()
    val blockCount = new AtomicLong()
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet.fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    val watcher = system.actorOf(Props(new NeutrinoWatcher(blockCount, wallet)))

    val (address, _) = getNewAddress(bitcoincli)
    val tx = sendToAddress(bitcoincli, address, 1.0)

    val listener = TestProbe()
    probe.send(watcher, WatchConfirmed(listener.ref, tx.txid, tx.txOut(0).publicKeyScript, 4, BITCOIN_FUNDING_DEPTHOK))
    generateBlocks(bitcoincli, 5)
    val confirmed = listener.expectMsgType[WatchEventConfirmed](20 seconds)
    assert(confirmed.tx.txid === tx.txid)

    system.stop(watcher)
  }

  test("watch for spent transactions") {
    val probe = TestProbe()
    val blockCount = new AtomicLong()
    val datadir: Path = BitcoinSTestAppConfig.tmpDir()
    val wallet = NeutrinoWallet.fromDatadir(datadir, Block.RegtestGenesisBlock.hash, overrideConfig = peerConfig)
    val watcher = system.actorOf(Props(new NeutrinoWatcher(blockCount, wallet)))

    val (address, priv) = getNewAddress(bitcoincli)
    val tx = sendToAddress(bitcoincli, address, 1.0)

    // find the output for the address we generated and create a tx that spends it
    val pos = tx.txOut.indexWhere(_.publicKeyScript == Script.write(Script.pay2wpkh(priv.publicKey)))
    assert(pos != -1)
    val spendingTx = {
      val tmp = Transaction(version = 2,
        txIn = TxIn(OutPoint(tx, pos), signatureScript = Nil, sequence = TxIn.SEQUENCE_FINAL) :: Nil,
        txOut = TxOut(tx.txOut(pos).amount - 1000.sat, publicKeyScript = Script.pay2wpkh(priv.publicKey)) :: Nil,
        lockTime = 0)
      val sig = Transaction.signInput(tmp, 0, Script.pay2pkh(priv.publicKey), SIGHASH_ALL, tx.txOut(pos).amount, SigVersion.SIGVERSION_WITNESS_V0, priv)
      val signedTx = tmp.updateWitness(0, ScriptWitness(sig :: priv.publicKey.value :: Nil))
      Transaction.correctlySpends(signedTx, Seq(tx), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
      signedTx
    }

    val listener = TestProbe()
    probe.send(watcher, WatchSpent(listener.ref, tx.txid, pos, tx.txOut(pos).publicKeyScript, BITCOIN_FUNDING_SPENT))
    listener.expectNoMsg(1 second)
    probe.send(bitcoincli, BitcoinReq("sendrawtransaction", spendingTx.toString))
    probe.expectMsgType[JValue]
    generateBlocks(bitcoincli, 2)
    listener.expectMsgType[WatchEventSpent](20 seconds)
    system.stop(watcher)
  }
  /*
    test("watch for mempool transactions (txs in mempool before we set the watch)") {
      val probe = TestProbe()
      val blockCount = new AtomicLong()
      val electrumClient = system.actorOf(Props(new ElectrumClientPool(blockCount, Set(electrumAddress))))
      probe.send(electrumClient, ElectrumClient.AddStatusListener(probe.ref))
      probe.expectMsgType[ElectrumClient.ElectrumReady]
      val watcher = system.actorOf(Props(new ElectrumWatcher(blockCount, electrumClient)))

      val (address, priv) = getNewAddress(bitcoincli)
      val tx = sendToAddress(bitcoincli, address, 1.0)
      val (tx1, tx2) = createUnspentTxChain(tx, priv)
      probe.send(bitcoincli, BitcoinReq("sendrawtransaction", tx1.toString()))
      probe.expectMsgType[JValue]
      probe.send(bitcoincli, BitcoinReq("sendrawtransaction", tx2.toString()))
      probe.expectMsgType[JValue]

      // wait until tx1 and tx2 are in the mempool (as seen by our ElectrumX server)
      awaitCond({
        probe.send(electrumClient, ElectrumClient.GetScriptHashHistory(ElectrumClient.computeScriptHash(tx2.txOut(0).publicKeyScript)))
        val ElectrumClient.GetScriptHashHistoryResponse(_, history) = probe.expectMsgType[ElectrumClient.GetScriptHashHistoryResponse]
        history.map(_.tx_hash).toSet == Set(tx.txid, tx1.txid, tx2.txid)
      }, max = 30 seconds, interval = 5 seconds)

      // then set a watch
      val listener = TestProbe()
      probe.send(watcher, WatchConfirmed(listener.ref, tx2.txid, tx2.txOut(0).publicKeyScript, 0, BITCOIN_FUNDING_DEPTHOK))
      val confirmed = listener.expectMsgType[WatchEventConfirmed](20 seconds)
      assert(confirmed.tx.txid === tx2.txid)
      system.stop(watcher)
    }

    test("watch for mempool transactions (txs not yet in the mempool when we set the watch)") {
      val probe = TestProbe()
      val blockCount = new AtomicLong()
      val electrumClient = system.actorOf(Props(new ElectrumClientPool(blockCount, Set(electrumAddress))))
      probe.send(electrumClient, ElectrumClient.AddStatusListener(probe.ref))
      probe.expectMsgType[ElectrumClient.ElectrumReady]
      val watcher = system.actorOf(Props(new ElectrumWatcher(blockCount, electrumClient)))

      val (address, priv) = getNewAddress(bitcoincli)
      val tx = sendToAddress(bitcoincli, address, 1.0)
      val (tx1, tx2) = createUnspentTxChain(tx, priv)

      // here we set the watch * before * we publish our transactions
      val listener = TestProbe()
      probe.send(watcher, WatchConfirmed(listener.ref, tx2.txid, tx2.txOut(0).publicKeyScript, 0, BITCOIN_FUNDING_DEPTHOK))
      probe.send(bitcoincli, BitcoinReq("sendrawtransaction", tx1.toString()))
      probe.expectMsgType[JValue]
      probe.send(bitcoincli, BitcoinReq("sendrawtransaction", tx2.toString()))
      probe.expectMsgType[JValue]

      val confirmed = listener.expectMsgType[WatchEventConfirmed](20 seconds)
      assert(confirmed.tx.txid === tx2.txid)
      system.stop(watcher)
    }
  */


}
