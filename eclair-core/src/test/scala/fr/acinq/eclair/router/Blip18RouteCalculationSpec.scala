package fr.acinq.eclair.router

import com.softwaremill.quicklens.ModifyPimp
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{Block, Satoshi, SatoshiLong}
import fr.acinq.eclair.payment.relay.Relayer.RelayFees
import fr.acinq.eclair.router.Graph.GraphStructure.{DirectedGraph, GraphEdge}
import fr.acinq.eclair.router.Graph.{RichWeight, WeightRatios}
import fr.acinq.eclair.router.RouteCalculation.findRoute
import fr.acinq.eclair.router.Router._
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.wire.protocol.{ChannelAnnouncement, ChannelUpdate, ChannelUpdateTlv, TlvStream}
import fr.acinq.eclair.{BlockHeight, CltvExpiry, CltvExpiryDelta, Features, MilliSatoshi, MilliSatoshiLong, RealShortChannelId, ShortChannelId, TestConstants, TimestampSecond, TimestampSecondLong, randomKey}
import org.scalatest.ParallelTestExecution
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits._

import scala.util.{Failure, Success}

class Blip18RouteCalculationSpec extends AnyFunSuite with ParallelTestExecution {

  import Blip18RouteCalculationSpec._

  val (a, b, c, d, e, f) = (randomKey().publicKey, randomKey().publicKey, randomKey().publicKey, randomKey().publicKey, randomKey().publicKey, randomKey().publicKey)

  test("calculate Blip18 simple route") {
    val g = DirectedGraph(List(
      makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
      makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(-1 msat), inboundFeeProportionalMillionth_opt = Some(-10)),
      makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
      makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
    ))

    val Success(route :: Nil) = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
    assert(route2Ids(route) == 1 :: 2 :: 3 :: 4 :: Nil)
    assert(route2Fees(route) == 101.msat :: 101.msat :: 101.msat :: Nil)
    assert(route.channelFee(false) == 303.msat)
  }

  test("check Blip18 fee against max pct properly") {
    // fee is acceptable if it is either:
    //  - below our maximum fee base
    //  - below our maximum fraction of the paid amount
    // here we have a maximum fee base of 1 msat, and all our updates have a base fee of 10 msat
    // so our fee will always be above the base fee, and we will always check that it is below our maximum percentage
    // of the amount being paid
    val routeParams = DEFAULT_ROUTE_PARAMS.modify(_.boundaries.maxFeeFlat).setTo(1 msat)
    val maxFee = routeParams.getMaxFee(DEFAULT_AMOUNT_MSAT)

    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 10 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(2L, b, c, 1000 msat, 30000, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(3L, c, d, 10 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 10 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val Failure(ex) = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, maxFee, numRoutes = 1, routeParams = routeParams, currentBlockHeight = BlockHeight(400000))
      assert(ex == RouteNotFound)
    }

    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 10 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(2L, b, c, 1000 msat, 30000, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(0 msat), inboundFeeProportionalMillionth_opt = Some(-10000)),
        makeEdge(3L, c, d, 10 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 10 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val Success(route :: Nil) = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, maxFee, numRoutes = 1, routeParams = routeParams, currentBlockHeight = BlockHeight(400000))
      assert(route2Ids(route) == 1 :: 2 :: 3 :: 4 :: Nil)
      assert(route2Fees(route) == 197990.msat :: 110.msat :: 110.msat :: Nil)
    }
  }

  test("calculate the shortest path (correct fees)") {
    val (a, b, c, d, e, f) = (
      PublicKey(hex"02999fa724ec3c244e4da52b4a91ad421dc96c9a810587849cd4b2469313519c73"), // a: source
      PublicKey(hex"03f1cb1af20fe9ccda3ea128e27d7c39ee27375c8480f11a87c17197e97541ca6a"),
      PublicKey(hex"0358e32d245ff5f5a3eb14c78c6f69c67cea7846bdf9aeeb7199e8f6fbb0306484"),
      PublicKey(hex"029e059b6780f155f38e83601969919aae631ddf6faed58fe860c72225eb327d7c"), // d: target
      PublicKey(hex"03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f"),
      PublicKey(hex"020c65be6f9252e85ae2fe9a46eed892cb89565e2157730e78311b1621a0db4b22")
    )

    // note: we don't actually use floating point numbers
    // cost(CD) = 10005 = amountMsat + 1 + (amountMsat * 400 / 1000000)
    // cost(BC) = 10009,0015 = (cost(CD) + 1 + (cost(CD) * 300 / 1000000)
    // cost(FD) = 10002 = amountMsat + 1 + (amountMsat * 100 / 1000000)
    // cost(EF) = 10007,0008 = cost(FD) + 1 + (cost(FD) * 400 / 1000000)
    // cost(AE) = 10007 -> A is source, shortest path found
    // cost(AB) = 10009
    //
    // The amounts that need to be sent through each edge are then:
    //
    //                 +--- A ---+
    // 10009,0015 msat |         | 10007,0008 msat
    //                 B         E
    //      10005 msat |         | 10002 msat
    //                 C         F
    //      10000 msat |         | 10000 msat
    //                 +--> D <--+

    val amount = 10000 msat
    val expectedCost = 10007 msat
    val graph = DirectedGraph(List(
      makeEdge(1L, a, b, feeBase = 1 msat, feeProportionalMillionth = 200, minHtlc = 0 msat),
      makeEdge(4L, a, e, feeBase = 1 msat, feeProportionalMillionth = 200, minHtlc = 0 msat),
      makeEdge(2L, b, c, feeBase = 1 msat, feeProportionalMillionth = 300, minHtlc = 0 msat),
      makeEdge(3L, c, d, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat),
      makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat),
      makeEdge(6L, f, d, feeBase = 1 msat, feeProportionalMillionth = 100, minHtlc = 0 msat)
    ))

    val Success(route :: Nil) = findRoute(graph, a, d, amount, maxFee = 7 msat, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
    val weightedPath = Graph.pathWeight(a, route2Edges(route), amount, BlockHeight(0), Left(NO_WEIGHT_RATIOS), includeLocalChannelCost = false)
    assert(route2Ids(route) == 4 :: 5 :: 6 :: Nil)
    assert(weightedPath.length == 3)
    assert(weightedPath.amount == expectedCost)
    assert(route.channelFee(false) == 7.msat)
    assert(route2Fees(route) == 5.msat :: 2.msat :: Nil)

    // update channel 5 so that it can route the final amount (10000) but not the amount + fees (10002)
    val graph1 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, maxHtlc = Some(10001 msat)))
    val graph2 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, capacity = 10 sat))
    val graph3 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, balance_opt = Some(10001 msat)))
    for (g <- Seq(graph1, graph2, graph3)) {
      val Success(route1 :: Nil) = findRoute(g, a, d, amount, maxFee = 10 msat, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(route2Ids(route1) == 1 :: 2 :: 3 :: Nil)
      assert(route1.channelFee(false) == 9.msat)
      assert(route2Fees(route1) == 4.msat :: 5.msat :: Nil)
    }

    // update channel 5 with inbound fees
    val graph4 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, inboundFeeBase_opt = Some(-1 msat), inboundFeeProportionalMillionth_opt = Some(-100)))
    val graph5 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, inboundFeeBase_opt = Some(-1 msat), inboundFeeProportionalMillionth_opt = Some(-200)))
    val graph6 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, inboundFeeBase_opt = Some(-1 msat), inboundFeeProportionalMillionth_opt = Some(-300)))
    val res = Seq(graph4, graph5, graph6).map(g => findRoute(g, a, d, amount, maxFee = 5 msat, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000)))
    assert(res.forall(_.isSuccess))
    val allRoutes = res.map(_.get)
    assert(allRoutes.forall(_.tail == Nil))
    val routes = allRoutes.map(_.head)

    assert(routes.forall(r => route2Ids(r) == 4 :: 5 :: 6 :: Nil ))
    assert(routes.map(_.channelFee(false)) == Seq(5 msat, 4 msat, 3 msat))
    assert(route2Fees(routes.toIndexedSeq(0)) == Seq(3.msat, 2.msat))
    assert(route2Fees(routes.toIndexedSeq(1)) == Seq(2.msat, 2.msat))
    assert(route2Fees(routes.toIndexedSeq(2)) == Seq(1.msat, 2.msat))

    // update channel 5 with positive inbound fees
    val graph7 = graph.addEdge(makeEdge(5L, e, f, feeBase = 1 msat, feeProportionalMillionth = 400, minHtlc = 0 msat, inboundFeeBase_opt = Some(1 msat), inboundFeeProportionalMillionth_opt = Some(600)))
    val Success(route7 :: Nil) = findRoute(graph7, a, d, amount, maxFee = 70 msat, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
    val weightedPath7 = Graph.pathWeight(a, route2Edges(route), amount, BlockHeight(0), Left(NO_WEIGHT_RATIOS), includeLocalChannelCost = false)
    assert(route2Ids(route7) == 1 :: 2 :: 3 :: Nil)
    assert(weightedPath7.length == 3)
    assert(weightedPath7.amount == expectedCost)
    assert(route7.channelFee(false) == 9.msat)
    assert(route2Fees(route7) == 4.msat :: 5.msat :: Nil)
  }

  test("calculate Blip18 simple route with a positive inbound fees channel") {
    // channels with positive inbound fees should be excluded
    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
        makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(1 msat), inboundFeeProportionalMillionth_opt = Some(10)),
        makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val res = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(res == Failure(RouteNotFound))
    }
    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
        makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(1 msat), inboundFeeProportionalMillionth_opt = Some(0)),
        makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val res = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(res == Failure(RouteNotFound))
    }
    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
        makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(0 msat), inboundFeeProportionalMillionth_opt = Some(10)),
        makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val res = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(res == Failure(RouteNotFound))
    }
    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
        makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(1 msat), inboundFeeProportionalMillionth_opt = Some(-10)),
        makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val res = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(res == Failure(RouteNotFound))
    }
    {
      val g = DirectedGraph(List(
        makeEdge(1L, a, b, 1 msat, 10, cltvDelta = CltvExpiryDelta(1), balance_opt = Some(DEFAULT_AMOUNT_MSAT * 2)),
        makeEdge(2L, b, c, 2 msat, 20, cltvDelta = CltvExpiryDelta(1), inboundFeeBase_opt = Some(-1 msat), inboundFeeProportionalMillionth_opt = Some(10)),
        makeEdge(3L, c, d, 1 msat, 10, cltvDelta = CltvExpiryDelta(1)),
        makeEdge(4L, d, e, 1 msat, 10, cltvDelta = CltvExpiryDelta(1))
      ))

      val res = findRoute(g, a, e, DEFAULT_AMOUNT_MSAT, DEFAULT_MAX_FEE, numRoutes = 1, routeParams = DEFAULT_ROUTE_PARAMS, currentBlockHeight = BlockHeight(400000))
      assert(res == Failure(RouteNotFound))
    }
  }
}

object Blip18RouteCalculationSpec {

  val noopBoundaries = { _: RichWeight => true }

  val DEFAULT_AMOUNT_MSAT = 10_000_000 msat
  val DEFAULT_MAX_FEE = 100_000 msat
  val DEFAULT_EXPIRY = CltvExpiry(TestConstants.defaultBlockHeight)
  val DEFAULT_CAPACITY = 100_000 sat

  val NO_WEIGHT_RATIOS: WeightRatios = WeightRatios(1, 0, 0, 0, RelayFees(0 msat, 0))
  val DEFAULT_ROUTE_PARAMS = PathFindingConf(
    randomize = false,
    boundaries = SearchBoundaries(21000 msat, 0.03, 6, CltvExpiryDelta(2016)),
    Left(NO_WEIGHT_RATIOS),
    MultiPartParams(1000 msat, 10),
    experimentName = "my-test-experiment",
    experimentPercentage = 100).getDefaultRouteParams

  val DUMMY_SIG = Transactions.PlaceHolderSig

  def makeChannel(shortChannelId: Long, nodeIdA: PublicKey, nodeIdB: PublicKey): ChannelAnnouncement = {
    val (nodeId1, nodeId2) = if (Announcements.isNode1(nodeIdA, nodeIdB)) (nodeIdA, nodeIdB) else (nodeIdB, nodeIdA)
    ChannelAnnouncement(DUMMY_SIG, DUMMY_SIG, DUMMY_SIG, DUMMY_SIG, Features.empty, Block.RegtestGenesisBlock.hash, RealShortChannelId(shortChannelId), nodeId1, nodeId2, randomKey().publicKey, randomKey().publicKey)
  }

  def makeEdge(shortChannelId: Long,
               nodeId1: PublicKey,
               nodeId2: PublicKey,
               feeBase: MilliSatoshi,
               feeProportionalMillionth: Int,
               minHtlc: MilliSatoshi = DEFAULT_AMOUNT_MSAT,
               maxHtlc: Option[MilliSatoshi] = None,
               cltvDelta: CltvExpiryDelta = CltvExpiryDelta(0),
               capacity: Satoshi = DEFAULT_CAPACITY,
               balance_opt: Option[MilliSatoshi] = None,
               inboundFeeBase_opt: Option[MilliSatoshi] = None,
               inboundFeeProportionalMillionth_opt: Option[Int] = None): GraphEdge = {
    val update = makeUpdateShort(ShortChannelId(shortChannelId), nodeId1, nodeId2, feeBase, feeProportionalMillionth, minHtlc, maxHtlc, cltvDelta, inboundFeeBase_opt = inboundFeeBase_opt, inboundFeeProportionalMillionth_opt = inboundFeeProportionalMillionth_opt)
    GraphEdge(ChannelDesc(RealShortChannelId(shortChannelId), nodeId1, nodeId2), HopRelayParams.FromAnnouncement(update), capacity, balance_opt)
  }

  def makeUpdateShort(shortChannelId: ShortChannelId, nodeId1: PublicKey, nodeId2: PublicKey, feeBase: MilliSatoshi, feeProportionalMillionth: Int, minHtlc: MilliSatoshi = DEFAULT_AMOUNT_MSAT, maxHtlc: Option[MilliSatoshi] = None, cltvDelta: CltvExpiryDelta = CltvExpiryDelta(0), timestamp: TimestampSecond = 0 unixsec, inboundFeeBase_opt: Option[MilliSatoshi] = None, inboundFeeProportionalMillionth_opt: Option[Int] = None): ChannelUpdate = {
    val tlvStream: TlvStream[ChannelUpdateTlv] = if (inboundFeeBase_opt.isDefined && inboundFeeProportionalMillionth_opt.isDefined) {
      TlvStream(ChannelUpdateTlv.Blip18InboundFee(inboundFeeBase_opt.get.toLong.toInt, inboundFeeProportionalMillionth_opt.get))
    } else {
      TlvStream.empty
    }
    ChannelUpdate(
      signature = DUMMY_SIG,
      chainHash = Block.RegtestGenesisBlock.hash,
      shortChannelId = shortChannelId,
      timestamp = timestamp,
      messageFlags = ChannelUpdate.MessageFlags(dontForward = false),
      channelFlags = ChannelUpdate.ChannelFlags(isEnabled = true, isNode1 = Announcements.isNode1(nodeId1, nodeId2)),
      cltvExpiryDelta = cltvDelta,
      htlcMinimumMsat = minHtlc,
      feeBaseMsat = feeBase,
      feeProportionalMillionths = feeProportionalMillionth,
      htlcMaximumMsat = maxHtlc.getOrElse(500_000_000 msat),
      tlvStream = tlvStream
    )
  }

  def hops2Ids(hops: Seq[ChannelHop]): Seq[Long] = hops.map(hop => hop.shortChannelId.toLong)

  def route2Ids(route: Route): Seq[Long] = hops2Ids(route.hops)

  def routes2Ids(routes: Seq[Route]): Set[Seq[Long]] = routes.map(route2Ids).toSet

  def route2Edges(route: Route): Seq[GraphEdge] = route.hops.map(hop => GraphEdge(ChannelDesc(hop.shortChannelId, hop.nodeId, hop.nextNodeId), hop.params, 0 sat, None))

  def route2Nodes(route: Route): Seq[(PublicKey, PublicKey)] = route.hops.map(hop => (hop.nodeId, hop.nextNodeId))

  def route2NodeIds(route: Route): Seq[PublicKey] = route.hops.head.nodeId +: route.hops.map(_.nextNodeId)

  def hops2Fees(amount: MilliSatoshi, hops: Seq[ChannelHop]): Seq[MilliSatoshi] = hops.map(hop => hop.fee(amount))

  def route2Fees(route: Route, includeLocalChannelCost: Boolean = false): Seq[MilliSatoshi] = hops2Fees(route.amount, if (includeLocalChannelCost) route.hops else route.hops.drop(1))
  
  def checkIgnoredChannels(routes: Seq[Route], shortChannelIds: Long*): Unit = {
    shortChannelIds.foreach(shortChannelId => routes.foreach(route => {
      assert(route.hops.forall(_.shortChannelId.toLong != shortChannelId), route)
    }))
  }

  def checkRouteAmounts(routes: Seq[Route], totalAmount: MilliSatoshi, maxFee: MilliSatoshi): Unit = {
    assert(routes.map(_.amount).sum == totalAmount, routes)
    assert(routes.map(_.channelFee(false)).sum <= maxFee, routes)
  }

}
