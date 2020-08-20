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

package fr.acinq.eclair.blockchain.bitcoins.rpc

import fr.acinq.bitcoin._
import fr.acinq.eclair.blockchain.bitcoind.rpc.{BitcoinJsonRPCClient, ExtendedBitcoinClient}
import org.bitcoins.core.api.chain.ChainQueryApi
import org.bitcoins.core.api.chain.ChainQueryApi.FilterResponse
import org.bitcoins.core.api.node.NodeApi
import org.bitcoins.core.gcs.{BlockFilter, FilterType, GolombFilter}
import org.bitcoins.core.protocol.blockchain.Block
import org.bitcoins.core.protocol.{BitcoinAddress, BlockStamp}
import org.bitcoins.core.util.{FutureUtil, Mutable}
import org.bitcoins.crypto.{DoubleSha256Digest, DoubleSha256DigestBE}
import org.bitcoins.node.NodeCallbacks
import org.json4s.JsonAST._

import scala.concurrent.{ExecutionContext, Future}

class BitcoinSBitcoinClient(override val rpcClient: BitcoinJsonRPCClient)(implicit ec: ExecutionContext) extends ExtendedBitcoinClient(rpcClient) with ChainQueryApi with NodeApi {

  private val callbacks = new Mutable(NodeCallbacks.empty)

  def nodeCallbacks: NodeCallbacks = callbacks.atomicGet

  def addCallbacks(newCallbacks: NodeCallbacks): BitcoinSBitcoinClient = {
    callbacks.atomicUpdate(newCallbacks)(_ + _)
    this
  }

  def getBlockHeight(hash: ByteVector32)(implicit ec: ExecutionContext): Future[Int] = {
    for {
      json <- rpcClient.invoke("getblockheader", hash.toHex, true)
      JInt(height) = json \ "height"
    } yield height.toInt
  }

  private def toEclairTx(tx: org.bitcoins.core.protocol.transaction.Transaction): Transaction = {
    Transaction.read(tx.bytes.toArray)
  }

  override def getBlockHeight(blockHash: DoubleSha256DigestBE): Future[Option[Int]] = {
    val bytes = ByteVector32(blockHash.bytes)
    getBlockHeight(bytes).map(Some(_))
  }

  override def getBestBlockHash(): Future[DoubleSha256DigestBE] = {
    rpcClient.invoke("getbestblockhash") collect {
      case JString(hash) => DoubleSha256DigestBE.fromHex(hash)
    }
  }

  override def getNumberOfConfirmations(blockHashOpt: DoubleSha256DigestBE): Future[Option[Int]] = {
    rpcClient.invoke("getblockheader", blockHashOpt.hex, true).map { json =>
      val JInt(confirmations) = json \ "confirmations"
      Some(confirmations.toInt)
    }.recoverWith(_ => Future.successful(None))
  }

  override def getFilterCount: Future[Int] = getBlockCount.map(_.toInt)

  override def getHeightByBlockStamp(blockStamp: BlockStamp): Future[Int] = blockStamp match {
    case blockHeight: BlockStamp.BlockHeight =>
      Future.successful(blockHeight.height)
    case blockHash: BlockStamp.BlockHash =>
      val bytes = ByteVector32(blockHash.hash.bytes)
      getBlockHeight(bytes)
    case blockTime: BlockStamp.BlockTime =>
      Future.failed(
        new UnsupportedOperationException(s"Not implemented: $blockTime"))
  }

  def getBlockHash(height: Int): Future[DoubleSha256DigestBE] = {
    rpcClient.invoke("getblockhash", height) collect {
      case JString(hash) => DoubleSha256DigestBE(hash)
    }
  }

  def getBlockFilter(hash: DoubleSha256DigestBE, filterType: FilterType): Future[GolombFilter] = {
    for {
      json <- rpcClient.invoke("getblockfilter", hash.hex, filterType.toString.toLowerCase)
      JString(filter) = json \ "filter"
      blockFilter = BlockFilter.fromHex(filter, hash.flip)
      _ <- nodeCallbacks.onCompactFiltersReceived.execute(Vector((hash.flip, blockFilter)))
    } yield blockFilter
  }

  def getBlock(hash: DoubleSha256DigestBE): Future[Block] = {
    rpcClient.invoke("getblock", hash.hex, 0) collect {
      case JString(block) => Block(block)
    }
  }

  def generateToAddress(numBlocks: Int, address: BitcoinAddress): Future[Vector[DoubleSha256DigestBE]] = {
    for {
      hashes <- rpcClient.invoke("generatetoaddress", numBlocks, address.toString, 1000000).map(json => json.extract[List[String]].map(DoubleSha256DigestBE.fromHex))
    } yield hashes.toVector
  }

  def generateToAddress(numBlocks: Int, address: String): Future[Vector[DoubleSha256DigestBE]] = {
    generateToAddress(numBlocks, BitcoinAddress.fromString(address))
  }

  override def getFiltersBetweenHeights(startHeight: Int, endHeight: Int): Future[Vector[ChainQueryApi.FilterResponse]] = {
    val allHeights = startHeight.to(endHeight)

    def f(range: Vector[Int]): Future[Vector[FilterResponse]] = {
      val filterFs = range.map { height =>
        for {
          hash <- getBlockHash(height)
          filter <- getBlockFilter(hash, FilterType.Basic)
        } yield {
          FilterResponse(filter, hash, height)
        }
      }
      Future.sequence(filterFs)
    }

    FutureUtil.batchExecute(elements = allHeights.toVector,
      f = f,
      init = Vector.empty,
      batchSize = 25)
  }


  override def epochSecondToBlockHeight(time: Long): Future[Int] = Future.successful(0)

  override def broadcastTransaction(transaction: org.bitcoins.core.protocol.transaction.Transaction): Future[Unit] =
    publishTransaction(toEclairTx(transaction)).map(_ => ())

  override def downloadBlocks(blockHashes: Vector[DoubleSha256Digest]): Future[Unit] = {
    FutureUtil.sequentially(blockHashes) { hash =>
      for {
        block <- getBlock(hash.flip)
        _ <- nodeCallbacks.onBlockReceived.execute(block)
      } yield ()
    }.map(_ => ())
  }
}
