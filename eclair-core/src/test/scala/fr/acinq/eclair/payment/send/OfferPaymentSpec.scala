/*
 * Copyright 2023 ACINQ SAS
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

package fr.acinq.eclair.payment.send

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe => TypedProbe}
import akka.actor.typed.ActorRef
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import fr.acinq.eclair.EncodedNodeId.ShortChannelIdDir
import fr.acinq.eclair.crypto.Sphinx.RouteBlinding
import fr.acinq.eclair.message.OnionMessages.RoutingStrategy.FindRoute
import fr.acinq.eclair.message.Postman
import fr.acinq.eclair.payment.send.OfferPayment._
import fr.acinq.eclair.payment.send.PaymentInitiator.SendPaymentToNode
import fr.acinq.eclair.payment.{Bolt12Invoice, PaymentBlindedRoute}
import fr.acinq.eclair.router.Router
import fr.acinq.eclair.router.Router.RouteParams
import fr.acinq.eclair.wire.protocol.MessageOnion.InvoicePayload
import fr.acinq.eclair.wire.protocol.OfferTypes.{InvoiceRequest, Offer, PaymentInfo}
import fr.acinq.eclair.wire.protocol.{OfferTypes, OnionMessagePayloadTlv, TlvStream}
import fr.acinq.eclair.{CltvExpiryDelta, EncodedNodeId, Features, MilliSatoshiLong, NodeParams, RealShortChannelId, TestConstants, randomBytes, randomBytes32, randomKey}
import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuiteLike
import scodec.bits.HexStringSyntax

import scala.concurrent.duration.DurationInt

class OfferPaymentSpec extends ScalaTestWithActorTestKit(ConfigFactory.load("application")) with FixtureAnyFunSuiteLike {

  case class FixtureParam(offerPayment: ActorRef[Command], nodeParams: NodeParams, postman: TypedProbe[Postman.Command], router: TestProbe, paymentInitiator: TestProbe, routeParams: RouteParams)

  override def withFixture(test: OneArgTest): Outcome = {
    val nodeParams = TestConstants.Alice.nodeParams
    val postman = TypedProbe[Postman.Command]("postman")
    val router = TestProbe("router")
    val paymentInitiator = TestProbe("paymentInitiator")
    val offerPayment = testKit.spawn(OfferPayment(nodeParams, postman.ref, router.ref, paymentInitiator.ref))
    val routeParams = nodeParams.routerConf.pathFindingExperimentConf.getRandomConf().getDefaultRouteParams
    try {
      withFixture(test.toNoArgTest(FixtureParam(offerPayment, nodeParams, postman, router, paymentInitiator, routeParams)))
    } finally {
      testKit.stop(offerPayment)
    }
  }

  implicit val classicSystem: ActorSystem = system.classicSystem

  test("basic offer payment") { f =>
    import f._

    val probe = TestProbe()
    val merchantKey = randomKey()

    val offer = Offer(None, Some("amountless offer"), merchantKey.publicKey, Features.empty, nodeParams.chainHash)
    offerPayment ! PayOffer(probe.ref, offer, 40_000_000 msat, 1, SendPaymentConfig(None, connectDirectly = false, 1, routeParams, blocking = false))
    val Postman.SendMessage(OfferTypes.RecipientNodeId(recipientId), FindRoute, message, expectsReply, replyTo) = postman.expectMessageType[Postman.SendMessage]
    assert(recipientId == merchantKey.publicKey)
    assert(message.get[OnionMessagePayloadTlv.InvoiceRequest].nonEmpty)
    assert(expectsReply)
    val Right(invoiceRequest) = InvoiceRequest.validate(message.get[OnionMessagePayloadTlv.InvoiceRequest].get.tlvs)

    val preimage = randomBytes32()
    val paymentRoute = PaymentBlindedRoute(RouteBlinding.create(randomKey(), Seq(merchantKey.publicKey), Seq(hex"7777")).route, PaymentInfo(0 msat, 0, CltvExpiryDelta(0), 0 msat, 1_000_000_000 msat, Features.empty))
    val invoice = Bolt12Invoice(invoiceRequest, preimage, merchantKey, 1 minute, Features.empty, Seq(paymentRoute))
    replyTo ! Postman.Response(InvoicePayload(TlvStream(OnionMessagePayloadTlv.Invoice(invoice.records)), TlvStream.empty))
    val send = paymentInitiator.expectMsgType[SendPaymentToNode]
    assert(send.invoice == invoice)

    TypedProbe().expectTerminated(offerPayment)
  }

  test("no reply to invoice request with retries") { f =>
    import f._

    val probe = TestProbe()
    val merchantKey = randomKey()

    val offer = Offer(None, Some("amountless offer"), merchantKey.publicKey, Features.empty, nodeParams.chainHash)
    offerPayment ! PayOffer(probe.ref, offer, 40_000_000 msat, 1, SendPaymentConfig(None, connectDirectly = false, 1, routeParams, blocking = false))
    for (_ <- 1 to nodeParams.onionMessageConfig.maxAttempts) {
      val Postman.SendMessage(OfferTypes.RecipientNodeId(recipientId), FindRoute, message, expectsReply, replyTo) = postman.expectMessageType[Postman.SendMessage]
      assert(recipientId == merchantKey.publicKey)
      assert(message.get[OnionMessagePayloadTlv.InvoiceRequest].nonEmpty)
      assert(expectsReply)
      val Right(invoiceRequest) = InvoiceRequest.validate(message.get[OnionMessagePayloadTlv.InvoiceRequest].get.tlvs)
      assert(invoiceRequest.isValid)
      assert(invoiceRequest.offer == offer)
      replyTo ! Postman.NoReply
    }
    probe.expectMsg(NoInvoiceResponse)
    paymentInitiator.expectNoMessage(50 millis)
    TypedProbe().expectTerminated(offerPayment)
  }

  test("invalid invoice") { f =>
    import f._

    val probe = TestProbe()
    val merchantKey = randomKey()

    val offer = Offer(None, Some("amountless offer"), merchantKey.publicKey, Features.empty, nodeParams.chainHash)
    offerPayment ! PayOffer(probe.ref, offer, 40_000_000 msat, 1, SendPaymentConfig(None, connectDirectly = false, 1, routeParams, blocking = false))
    val Postman.SendMessage(OfferTypes.RecipientNodeId(recipientId), FindRoute, message, expectsReply, replyTo) = postman.expectMessageType[Postman.SendMessage]
    assert(recipientId == merchantKey.publicKey)
    assert(message.get[OnionMessagePayloadTlv.InvoiceRequest].nonEmpty)
    assert(expectsReply)
    val Right(invoiceRequest) = InvoiceRequest.validate(message.get[OnionMessagePayloadTlv.InvoiceRequest].get.tlvs)

    val preimage = randomBytes32()
    val paymentRoute = PaymentBlindedRoute(RouteBlinding.create(randomKey(), Seq(merchantKey.publicKey), Seq(hex"7777")).route, PaymentInfo(0 msat, 0, CltvExpiryDelta(0), 0 msat, 1_000_000_000 msat, Features.empty))
    val invoice = Bolt12Invoice(invoiceRequest, preimage, randomKey(), 1 minute, Features.empty, Seq(paymentRoute))
    replyTo ! Postman.Response(InvoicePayload(TlvStream(OnionMessagePayloadTlv.Invoice(invoice.records)), TlvStream.empty))

    probe.expectMsgType[InvalidInvoiceResponse]
    paymentInitiator.expectNoMessage(50 millis)

    TypedProbe().expectTerminated(offerPayment)
  }

  test("resolve compact paths") { f =>
    import f._

    val probe = TestProbe()
    val merchantKey = randomKey()

    val offer = Offer(None, Some("offer"), merchantKey.publicKey, Features.empty, nodeParams.chainHash)
    offerPayment ! PayOffer(probe.ref, offer, 40_000_000 msat, 1, SendPaymentConfig(None, connectDirectly = false, 1, routeParams, blocking = false))
    val Postman.SendMessage(OfferTypes.RecipientNodeId(recipientId), FindRoute, message, expectsReply, replyTo) = postman.expectMessageType[Postman.SendMessage]
    assert(recipientId == merchantKey.publicKey)
    assert(message.get[OnionMessagePayloadTlv.InvoiceRequest].nonEmpty)
    assert(expectsReply)
    val Right(invoiceRequest) = InvoiceRequest.validate(message.get[OnionMessagePayloadTlv.InvoiceRequest].get.tlvs)

    val preimage = randomBytes32()
    val blindedRoutes = Seq.fill(6)(RouteBlinding.create(randomKey(), Seq.fill(3)(randomKey().publicKey), Seq.fill(3)(randomBytes(10))).route)
    val paymentRoutes = Seq(
      PaymentBlindedRoute(blindedRoutes(0), PaymentInfo(0 msat, 0, CltvExpiryDelta(0), 0 msat, 1_000_000_000 msat, Features.empty)),
      PaymentBlindedRoute(blindedRoutes(1).copy(introductionNodeId = ShortChannelIdDir(isNode1 = true, RealShortChannelId(11111))), PaymentInfo(1 msat, 11, CltvExpiryDelta(111), 0 msat, 1_000_000_000 msat, Features.empty)),
      PaymentBlindedRoute(blindedRoutes(2), PaymentInfo(2 msat, 22, CltvExpiryDelta(222), 0 msat, 1_000_000_000 msat, Features.empty)),
      PaymentBlindedRoute(blindedRoutes(3).copy(introductionNodeId = ShortChannelIdDir(isNode1 = false, RealShortChannelId(33333))), PaymentInfo(3 msat, 33, CltvExpiryDelta(333), 0 msat, 1_000_000_000 msat, Features.empty)),
      PaymentBlindedRoute(blindedRoutes(4).copy(introductionNodeId = ShortChannelIdDir(isNode1 = false, RealShortChannelId(44444))), PaymentInfo(4 msat, 44, CltvExpiryDelta(444), 0 msat, 1_000_000_000 msat, Features.empty)),
      PaymentBlindedRoute(blindedRoutes(5), PaymentInfo(5 msat, 55, CltvExpiryDelta(555), 0 msat, 1_000_000_000 msat, Features.empty)),
    )
    val invoice = Bolt12Invoice(invoiceRequest, preimage, merchantKey, 1 minute, Features.empty, paymentRoutes)
    replyTo ! Postman.Response(InvoicePayload(TlvStream(OnionMessagePayloadTlv.Invoice(invoice.records)), TlvStream.empty))

    val getNode1 = router.expectMsgType[Router.GetNodeId]
    assert(getNode1.isNode1)
    assert(getNode1.shortChannelId == RealShortChannelId(11111))
    getNode1.replyTo ! Some(blindedRoutes(1).introductionNodeId.asInstanceOf[EncodedNodeId.Plain].publicKey)

    val getNode3 = router.expectMsgType[Router.GetNodeId]
    assert(!getNode3.isNode1)
    assert(getNode3.shortChannelId == RealShortChannelId(33333))
    getNode3.replyTo ! None

    val getNode4 = router.expectMsgType[Router.GetNodeId]
    assert(!getNode4.isNode1)
    assert(getNode4.shortChannelId == RealShortChannelId(44444))
    getNode4.replyTo ! Some(blindedRoutes(4).introductionNodeId.asInstanceOf[EncodedNodeId.Plain].publicKey)

    val send = paymentInitiator.expectMsgType[SendPaymentToNode]
    assert(send.invoice == invoice)
    assert(send.resolvedPaths.map(_.introductionNodeId) == Seq(blindedRoutes(0), blindedRoutes(1), blindedRoutes(2), blindedRoutes(4), blindedRoutes(5)).map(_.introductionNodeId.asInstanceOf[EncodedNodeId.Plain].publicKey))
    assert(send.resolvedPaths.map(_.blindedPath.paymentInfo.feeBase) == Seq(0 msat, 1 msat, 2 msat, 4 msat, 5 msat))

    TypedProbe().expectTerminated(offerPayment)
  }

  test("offer with implicit node id") { f =>
    import f._

    val probe = TestProbe()

    val merchantKey = randomKey()
    val route = RouteBlinding.create(randomKey(), Seq.fill(2)(randomKey().publicKey) :+ merchantKey.publicKey, Seq.fill(3)(randomBytes(10)))
    val offer = Offer.withPaths(None, Some("implicit node id"), Seq(route.route), Features.empty, nodeParams.chainHash)

    offerPayment ! PayOffer(probe.ref, offer, 10_000_000 msat, 1, SendPaymentConfig(None, connectDirectly = false, 1, routeParams, blocking = false))
    val Postman.SendMessage(OfferTypes.BlindedPath(blindedRoute), FindRoute, message, expectsReply, replyTo) = postman.expectMessageType[Postman.SendMessage]
    assert(blindedRoute == route.route)
    assert(message.get[OnionMessagePayloadTlv.InvoiceRequest].nonEmpty)
    assert(expectsReply)
    val Right(invoiceRequest) = InvoiceRequest.validate(message.get[OnionMessagePayloadTlv.InvoiceRequest].get.tlvs)

    val preimage = randomBytes32()
    val paymentRoute = PaymentBlindedRoute(RouteBlinding.create(randomKey(), Seq(merchantKey.publicKey), Seq(hex"7777")).route, PaymentInfo(0 msat, 0, CltvExpiryDelta(0), 0 msat, 1_000_000_000 msat, Features.empty))
    val blindedMerchantKey = RouteBlinding.derivePrivateKey(merchantKey, route.lastBlinding)
    val invoice = Bolt12Invoice(invoiceRequest, preimage, blindedMerchantKey, 1 minute, Features.empty, Seq(paymentRoute))
    replyTo ! Postman.Response(InvoicePayload(TlvStream(OnionMessagePayloadTlv.Invoice(invoice.records)), TlvStream.empty))
    val send = paymentInitiator.expectMsgType[SendPaymentToNode]
    assert(send.invoice == invoice)
    TypedProbe().expectTerminated(offerPayment)
  }
}
