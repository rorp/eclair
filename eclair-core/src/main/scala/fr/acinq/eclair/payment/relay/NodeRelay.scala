/*
 * Copyright 2020 ACINQ SAS
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

package fr.acinq.eclair.payment.relay

import akka.actor.typed.Behavior
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.adapter.{TypedActorContextOps, TypedActorRefOps}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.{ActorRef, typed}
import com.softwaremill.quicklens.ModifyPimp
import fr.acinq.bitcoin.scalacompat.ByteVector32
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.eclair.channel.{CMD_FAIL_HTLC, CMD_FULFILL_HTLC, Upstream}
import fr.acinq.eclair.db.PendingCommandsDb
import fr.acinq.eclair.io.PeerReadyNotifier
import fr.acinq.eclair.payment.IncomingPaymentPacket.NodeRelayPacket
import fr.acinq.eclair.payment.Monitoring.{Metrics, Tags}
import fr.acinq.eclair.payment._
import fr.acinq.eclair.payment.receive.MultiPartPaymentFSM
import fr.acinq.eclair.payment.receive.MultiPartPaymentFSM.HtlcPart
import fr.acinq.eclair.payment.send.BlindedPathsResolver.{Resolve, ResolvedPath}
import fr.acinq.eclair.payment.send.MultiPartPaymentLifecycle.{PreimageReceived, SendMultiPartPayment}
import fr.acinq.eclair.payment.send.PaymentInitiator.SendPaymentConfig
import fr.acinq.eclair.payment.send.PaymentLifecycle.SendPaymentToNode
import fr.acinq.eclair.payment.send._
import fr.acinq.eclair.router.Router.RouteParams
import fr.acinq.eclair.router.{BalanceTooLow, RouteNotFound}
import fr.acinq.eclair.wire.protocol.PaymentOnion.IntermediatePayload
import fr.acinq.eclair.wire.protocol._
import fr.acinq.eclair.{CltvExpiry, EncodedNodeId, Features, Logs, MilliSatoshi, NodeParams, TimestampMilli, UInt64, nodeFee, randomBytes32}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable.Queue

/**
 * It [[NodeRelay]] aggregates incoming HTLCs (in case multi-part was used upstream) and then forwards the requested amount (using the
 * router to find a route to the remote node and potentially splitting the payment using multi-part).
 */
object NodeRelay {

  // @formatter:off
  sealed trait Command
  case class Relay(nodeRelayPacket: IncomingPaymentPacket.NodeRelayPacket, originNode: PublicKey) extends Command
  case object Stop extends Command
  private case class WrappedMultiPartExtraPaymentReceived(mppExtraReceived: MultiPartPaymentFSM.ExtraPaymentReceived[HtlcPart]) extends Command
  private case class WrappedMultiPartPaymentFailed(mppFailed: MultiPartPaymentFSM.MultiPartPaymentFailed) extends Command
  private case class WrappedMultiPartPaymentSucceeded(mppSucceeded: MultiPartPaymentFSM.MultiPartPaymentSucceeded) extends Command
  private case class WrappedPreimageReceived(preimageReceived: PreimageReceived) extends Command
  private case class WrappedPaymentSent(paymentSent: PaymentSent) extends Command
  private case class WrappedPaymentFailed(paymentFailed: PaymentFailed) extends Command
  private case class WrappedPeerReadyResult(result: PeerReadyNotifier.Result) extends Command
  private case class WrappedResolvedPaths(resolved: Seq[ResolvedPath]) extends Command
  // @formatter:on

  trait OutgoingPaymentFactory {
    def spawnOutgoingPayFSM(context: ActorContext[NodeRelay.Command], cfg: SendPaymentConfig, multiPart: Boolean): ActorRef
  }

  case class SimpleOutgoingPaymentFactory(nodeParams: NodeParams, router: ActorRef, register: ActorRef) extends OutgoingPaymentFactory {
    val paymentFactory = PaymentInitiator.SimplePaymentFactory(nodeParams, router, register)

    override def spawnOutgoingPayFSM(context: ActorContext[Command], cfg: SendPaymentConfig, multiPart: Boolean): ActorRef = {
      if (multiPart) {
        context.toClassic.actorOf(MultiPartPaymentLifecycle.props(nodeParams, cfg, publishPreimage = true, router, paymentFactory))
      } else {
        context.toClassic.actorOf(PaymentLifecycle.props(nodeParams, cfg, router, register))
      }
    }
  }

  def apply(nodeParams: NodeParams,
            parent: typed.ActorRef[NodeRelayer.Command],
            register: ActorRef,
            relayId: UUID,
            nodeRelayPacket: NodeRelayPacket,
            outgoingPaymentFactory: OutgoingPaymentFactory,
            router: ActorRef): Behavior[Command] =
    Behaviors.setup { context =>
      val paymentHash = nodeRelayPacket.add.paymentHash
      val totalAmountIn = nodeRelayPacket.outerPayload.totalAmount
      Behaviors.withMdc(Logs.mdc(
        category_opt = Some(Logs.LogCategory.PAYMENT),
        parentPaymentId_opt = Some(relayId), // for a node relay, we use the same identifier for the whole relay itself, and the outgoing payment
        paymentHash_opt = Some(paymentHash))) {
        context.log.debug("relaying payment relayId={}", relayId)
        val mppFsmAdapters = {
          context.messageAdapter[MultiPartPaymentFSM.ExtraPaymentReceived[HtlcPart]](WrappedMultiPartExtraPaymentReceived)
          context.messageAdapter[MultiPartPaymentFSM.MultiPartPaymentFailed](WrappedMultiPartPaymentFailed)
          context.messageAdapter[MultiPartPaymentFSM.MultiPartPaymentSucceeded](WrappedMultiPartPaymentSucceeded)
        }.toClassic
        val incomingPaymentHandler = context.actorOf(MultiPartPaymentFSM.props(nodeParams, paymentHash, totalAmountIn, mppFsmAdapters))
        val nextPacket_opt = nodeRelayPacket match {
          case IncomingPaymentPacket.RelayToTrampolinePacket(_, _, _, nextPacket) => Some(nextPacket)
          case _: IncomingPaymentPacket.RelayToBlindedPathsPacket => None
        }
        new NodeRelay(nodeParams, parent, register, relayId, paymentHash, nodeRelayPacket.outerPayload.paymentSecret, context, outgoingPaymentFactory, router)
          .receiving(Queue.empty, nodeRelayPacket.innerPayload, nextPacket_opt, incomingPaymentHandler)
      }
    }

  private def validateRelay(nodeParams: NodeParams, upstream: Upstream.Hot.Trampoline, payloadOut: IntermediatePayload.NodeRelay): Option[FailureMessage] = {
    val fee = nodeFee(nodeParams.relayParams.minTrampolineFees, payloadOut.amountToForward)
    if (upstream.amountIn - payloadOut.amountToForward < fee) {
      Some(TrampolineFeeInsufficient())
    } else if (upstream.expiryIn - payloadOut.outgoingCltv < nodeParams.channelConf.expiryDelta) {
      Some(TrampolineExpiryTooSoon())
    } else if (payloadOut.outgoingCltv <= CltvExpiry(nodeParams.currentBlockHeight)) {
      Some(TrampolineExpiryTooSoon())
    } else if (payloadOut.amountToForward <= MilliSatoshi(0)) {
      Some(InvalidOnionPayload(UInt64(2), 0))
    } else {
      payloadOut match {
        // If we're relaying a standard payment to a non-trampoline recipient, we need the payment secret.
        case payloadOut: IntermediatePayload.NodeRelay.Standard if payloadOut.invoiceFeatures.isDefined && payloadOut.paymentSecret.isEmpty => Some(InvalidOnionPayload(UInt64(8), 0))
        case _: IntermediatePayload.NodeRelay.Standard => None
        case _: IntermediatePayload.NodeRelay.ToBlindedPaths => None
      }
    }
  }

  /** This function identifies whether the next node is a wallet node directly connected to us, and returns its node_id. */
  private def nextWalletNodeId(nodeParams: NodeParams, recipient: Recipient): Option[PublicKey] = {
    recipient match {
      // These two recipients are only used when we're the payment initiator.
      case _: SpontaneousRecipient => None
      case _: TrampolineRecipient => None
      // When relaying to a trampoline node, the next node may be a wallet node directly connected to us, but we don't
      // want to have false positives. Feature branches should check an internal DB/cache to confirm.
      case r: ClearRecipient if r.nextTrampolineOnion_opt.nonEmpty => None
      // If we're relaying to a non-trampoline recipient, it's never a wallet node.
      case _: ClearRecipient => None
      // When using blinded paths, we may be the introduction node for a wallet node directly connected to us.
      case r: BlindedRecipient => r.blindedHops.head.resolved.route match {
        case BlindedPathsResolver.PartialBlindedRoute(walletNodeId: EncodedNodeId.WithPublicKey.Wallet, _, _) => Some(walletNodeId.publicKey)
        case _ => None
      }
    }
  }

  /** Compute route params that honor our fee and cltv requirements. */
  private def computeRouteParams(nodeParams: NodeParams, amountIn: MilliSatoshi, expiryIn: CltvExpiry, amountOut: MilliSatoshi, expiryOut: CltvExpiry): RouteParams = {
    nodeParams.routerConf.pathFindingExperimentConf.getRandomConf().getDefaultRouteParams
      .modify(_.boundaries.maxFeeProportional).setTo(0) // we disable percent-based max fee calculation, we're only interested in collecting our node fee
      .modify(_.boundaries.maxCltv).setTo(expiryIn - expiryOut)
      .modify(_.boundaries.maxFeeFlat).setTo(amountIn - amountOut)
      .modify(_.includeLocalChannelCost).setTo(true)
  }

  /**
   * This helper method translates relaying errors (returned by the downstream nodes) to a BOLT 4 standard error that we
   * should return upstream.
   */
  private def translateError(nodeParams: NodeParams, failures: Seq[PaymentFailure], upstream: Upstream.Hot.Trampoline, nextPayload: IntermediatePayload.NodeRelay): Option[FailureMessage] = {
    val routeNotFound = failures.collectFirst { case f@LocalFailure(_, _, RouteNotFound) => f }.nonEmpty
    val routingFeeHigh = upstream.amountIn - nextPayload.amountToForward >= nodeFee(nodeParams.relayParams.minTrampolineFees, nextPayload.amountToForward) * 5
    failures match {
      case Nil => None
      case LocalFailure(_, _, BalanceTooLow) :: Nil if routingFeeHigh =>
        // We have direct channels to the target node, but not enough outgoing liquidity to use those channels.
        // The routing fee proposed by the sender was high enough to find alternative, indirect routes, but didn't yield
        // any result so we tell them that we don't have enough outgoing liquidity at the moment.
        Some(TemporaryNodeFailure())
      case LocalFailure(_, _, BalanceTooLow) :: Nil => Some(TrampolineFeeInsufficient()) // a higher fee/cltv may find alternative, indirect routes
      case _ if routeNotFound => Some(TrampolineFeeInsufficient()) // if we couldn't find routes, it's likely that the fee/cltv was insufficient
      case _ =>
        // Otherwise, we try to find a downstream error that we could decrypt.
        val outgoingNodeFailure = nextPayload match {
          case nextPayload: IntermediatePayload.NodeRelay.Standard => failures.collectFirst { case RemoteFailure(_, _, e) if e.originNode == nextPayload.outgoingNodeId => e.failureMessage }
          // When using blinded paths, we will never get a failure from the final node (for privacy reasons).
          case _: IntermediatePayload.NodeRelay.ToBlindedPaths => None
        }
        val otherNodeFailure = failures.collectFirst { case RemoteFailure(_, _, e) => e.failureMessage }
        val failure = outgoingNodeFailure.getOrElse(otherNodeFailure.getOrElse(TemporaryNodeFailure()))
        Some(failure)
    }
  }

}

/**
 * see https://doc.akka.io/docs/akka/current/typed/style-guide.html#passing-around-too-many-parameters
 */
class NodeRelay private(nodeParams: NodeParams,
                        parent: akka.actor.typed.ActorRef[NodeRelayer.Command],
                        register: ActorRef,
                        relayId: UUID,
                        paymentHash: ByteVector32,
                        paymentSecret: ByteVector32,
                        context: ActorContext[NodeRelay.Command],
                        outgoingPaymentFactory: NodeRelay.OutgoingPaymentFactory,
                        router: ActorRef) {

  import NodeRelay._

  /**
   * We start by aggregating an incoming HTLC set. Once we received the whole set, we will compute a route to the next
   * trampoline node and forward the payment.
   *
   * @param htlcs          received incoming HTLCs for this set.
   * @param nextPayload    relay instructions (should be identical across HTLCs in this set).
   * @param nextPacket_opt trampoline onion to relay to the next trampoline node.
   * @param handler        actor handling the aggregation of the incoming HTLC set.
   */
  private def receiving(htlcs: Queue[Upstream.Hot.Channel], nextPayload: IntermediatePayload.NodeRelay, nextPacket_opt: Option[OnionRoutingPacket], handler: ActorRef): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Relay(packet: IncomingPaymentPacket.NodeRelayPacket, originNode) =>
        require(packet.outerPayload.paymentSecret == paymentSecret, "payment secret mismatch")
        context.log.debug("forwarding incoming htlc #{} from channel {} to the payment FSM", packet.add.id, packet.add.channelId)
        handler ! MultiPartPaymentFSM.HtlcPart(packet.outerPayload.totalAmount, packet.add)
        receiving(htlcs :+ Upstream.Hot.Channel(packet.add.removeUnknownTlvs(), TimestampMilli.now(), originNode), nextPayload, nextPacket_opt, handler)
      case WrappedMultiPartPaymentFailed(MultiPartPaymentFSM.MultiPartPaymentFailed(_, failure, parts)) =>
        context.log.warn("could not complete incoming multi-part payment (parts={} paidAmount={} failure={})", parts.size, parts.map(_.amount).sum, failure)
        Metrics.recordPaymentRelayFailed(failure.getClass.getSimpleName, Tags.RelayType.Trampoline)
        parts.collect { case p: MultiPartPaymentFSM.HtlcPart => rejectHtlc(p.htlc.id, p.htlc.channelId, p.amount, Some(failure)) }
        stopping()
      case WrappedMultiPartPaymentSucceeded(MultiPartPaymentFSM.MultiPartPaymentSucceeded(_, parts)) =>
        context.log.info("completed incoming multi-part payment with parts={} paidAmount={}", parts.size, parts.map(_.amount).sum)
        val upstream = Upstream.Hot.Trampoline(htlcs)
        validateRelay(nodeParams, upstream, nextPayload) match {
          case Some(failure) =>
            context.log.warn(s"rejecting trampoline payment reason=$failure")
            rejectPayment(upstream, Some(failure))
            stopping()
          case None =>
            resolveNextNode(upstream, nextPayload, nextPacket_opt)
        }
    }

  /** Once we've fully received the incoming HTLC set, we must identify the next node before forwarding the payment. */
  private def resolveNextNode(upstream: Upstream.Hot.Trampoline, nextPayload: IntermediatePayload.NodeRelay, nextPacket_opt: Option[OnionRoutingPacket]): Behavior[Command] = {
    nextPayload match {
      case payloadOut: IntermediatePayload.NodeRelay.Standard =>
        // If invoice features are provided in the onion, the sender is asking us to relay to a non-trampoline recipient.
        payloadOut.invoiceFeatures match {
          case Some(features) =>
            val extraEdges = payloadOut.invoiceRoutingInfo.getOrElse(Nil).flatMap(Bolt11Invoice.toExtraEdges(_, payloadOut.outgoingNodeId))
            val paymentSecret = payloadOut.paymentSecret.get // NB: we've verified that there was a payment secret in validateRelay
            val recipient = ClearRecipient(payloadOut.outgoingNodeId, Features(features).invoiceFeatures(), payloadOut.amountToForward, payloadOut.outgoingCltv, paymentSecret, extraEdges, payloadOut.paymentMetadata)
            context.log.debug("forwarding payment to non-trampoline recipient {}", recipient.nodeId)
            ensureRecipientReady(upstream, recipient, nextPayload, None)
          case None =>
            val paymentSecret = randomBytes32() // we generate a new secret to protect against probing attacks
            val recipient = ClearRecipient(payloadOut.outgoingNodeId, Features.empty, payloadOut.amountToForward, payloadOut.outgoingCltv, paymentSecret, nextTrampolineOnion_opt = nextPacket_opt)
            context.log.debug("forwarding payment to the next trampoline node {}", recipient.nodeId)
            ensureRecipientReady(upstream, recipient, nextPayload, nextPacket_opt)
        }
      case payloadOut: IntermediatePayload.NodeRelay.ToBlindedPaths =>
        // Blinded paths in Bolt 12 invoices may encode the introduction node with an scid and a direction: we need to
        // resolve that to a nodeId in order to reach that introduction node and use the blinded path.
        // If we are the introduction node ourselves, we'll also need to decrypt the onion and identify the next node.
        context.spawnAnonymous(BlindedPathsResolver(nodeParams, paymentHash, router, register)) ! Resolve(context.messageAdapter[Seq[ResolvedPath]](WrappedResolvedPaths), payloadOut.outgoingBlindedPaths)
        Behaviors.receiveMessagePartial {
          rejectExtraHtlcPartialFunction orElse {
            case WrappedResolvedPaths(resolved) if resolved.isEmpty =>
              context.log.warn("rejecting trampoline payment to blinded paths: no usable blinded path")
              rejectPayment(upstream, Some(UnknownNextPeer()))
              stopping()
            case WrappedResolvedPaths(resolved) =>
              // We don't have access to the invoice: we use the only node_id that somewhat makes sense for the recipient.
              val blindedNodeId = resolved.head.route.blindedNodeIds.last
              val recipient = BlindedRecipient.fromPaths(blindedNodeId, Features(payloadOut.invoiceFeatures).invoiceFeatures(), payloadOut.amountToForward, payloadOut.outgoingCltv, resolved, Set.empty)
              context.log.debug("forwarding payment to blinded recipient {}", recipient.nodeId)
              ensureRecipientReady(upstream, recipient, nextPayload, nextPacket_opt)
          }
        }
    }
  }

  /**
   * The next node may be a mobile wallet directly connected to us: in that case, we'll need to wake them up before
   * relaying the payment.
   */
  private def ensureRecipientReady(upstream: Upstream.Hot.Trampoline, recipient: Recipient, nextPayload: IntermediatePayload.NodeRelay, nextPacket_opt: Option[OnionRoutingPacket]): Behavior[Command] = {
    nextWalletNodeId(nodeParams, recipient) match {
      case Some(walletNodeId) if nodeParams.peerWakeUpConfig.enabled => waitForPeerReady(upstream, walletNodeId, recipient, nextPayload, nextPacket_opt)
      case _ => relay(upstream, recipient, nextPayload, nextPacket_opt)
    }
  }

  /**
   * The next node is the payment recipient. They are directly connected to us and may be offline. We try to wake them
   * up and will relay the payment once they're connected and channels are reestablished.
   */
  private def waitForPeerReady(upstream: Upstream.Hot.Trampoline, walletNodeId: PublicKey, recipient: Recipient, nextPayload: IntermediatePayload.NodeRelay, nextPacket_opt: Option[OnionRoutingPacket]): Behavior[Command] = {
    context.log.info("trying to wake up next peer (nodeId={})", walletNodeId)
    val notifier = context.spawnAnonymous(PeerReadyNotifier(walletNodeId, timeout_opt = Some(Left(nodeParams.peerWakeUpConfig.timeout))))
    notifier ! PeerReadyNotifier.NotifyWhenPeerReady(context.messageAdapter(WrappedPeerReadyResult))
    Behaviors.receiveMessagePartial {
      rejectExtraHtlcPartialFunction orElse {
        case WrappedPeerReadyResult(_: PeerReadyNotifier.PeerUnavailable) =>
          context.log.warn("rejecting payment: failed to wake-up remote peer")
          rejectPayment(upstream, Some(UnknownNextPeer()))
          stopping()
        case WrappedPeerReadyResult(_: PeerReadyNotifier.PeerReady) =>
          relay(upstream, recipient, nextPayload, nextPacket_opt)
      }
    }
  }

  /** Relay the payment to the next identified node: this is similar to sending an outgoing payment. */
  private def relay(upstream: Upstream.Hot.Trampoline, recipient: Recipient, payloadOut: IntermediatePayload.NodeRelay, packetOut_opt: Option[OnionRoutingPacket]): Behavior[Command] = {
    context.log.debug("relaying trampoline payment (amountIn={} expiryIn={} amountOut={} expiryOut={})", upstream.amountIn, upstream.expiryIn, payloadOut.amountToForward, payloadOut.outgoingCltv)
    val confidence = (upstream.received.map(_.add.endorsement).min + 0.5) / 8
    val paymentCfg = SendPaymentConfig(relayId, relayId, None, paymentHash, recipient.nodeId, upstream, None, None, storeInDb = false, publishEvent = false, recordPathFindingMetrics = true, confidence)
    val routeParams = computeRouteParams(nodeParams, upstream.amountIn, upstream.expiryIn, payloadOut.amountToForward, payloadOut.outgoingCltv)
    // If the next node is using trampoline, we assume that they support MPP.
    val useMultiPart = recipient.features.hasFeature(Features.BasicMultiPartPayment) || packetOut_opt.nonEmpty
    val payFsmAdapters = {
      context.messageAdapter[PreimageReceived](WrappedPreimageReceived)
      context.messageAdapter[PaymentSent](WrappedPaymentSent)
      context.messageAdapter[PaymentFailed](WrappedPaymentFailed)
    }.toClassic
    val payment = if (useMultiPart) {
      SendMultiPartPayment(payFsmAdapters, recipient, nodeParams.maxPaymentAttempts, routeParams)
    } else {
      SendPaymentToNode(payFsmAdapters, recipient, nodeParams.maxPaymentAttempts, routeParams)
    }
    val payFSM = outgoingPaymentFactory.spawnOutgoingPayFSM(context, paymentCfg, useMultiPart)
    payFSM ! payment
    sending(upstream, payloadOut, recipient, TimestampMilli.now(), fulfilledUpstream = false)
  }

  /**
   * Once the payment is forwarded, we're waiting for fail/fulfill responses from downstream nodes.
   *
   * @param upstream          complete HTLC set received.
   * @param nextPayload       relay instructions.
   * @param fulfilledUpstream true if we already fulfilled the payment upstream.
   */
  private def sending(upstream: Upstream.Hot.Trampoline,
                      nextPayload: IntermediatePayload.NodeRelay,
                      recipient: Recipient,
                      startedAt: TimestampMilli,
                      fulfilledUpstream: Boolean): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      rejectExtraHtlcPartialFunction orElse {
        // this is the fulfill that arrives from downstream channels
        case WrappedPreimageReceived(PreimageReceived(_, paymentPreimage)) =>
          if (!fulfilledUpstream) {
            // We want to fulfill upstream as soon as we receive the preimage (even if not all HTLCs have fulfilled downstream).
            context.log.debug("got preimage from downstream")
            fulfillPayment(upstream, paymentPreimage)
            sending(upstream, nextPayload, recipient, startedAt, fulfilledUpstream = true)
          } else {
            // we don't want to fulfill multiple times
            Behaviors.same
          }
        case WrappedPaymentSent(paymentSent) =>
          context.log.debug("trampoline payment fully resolved downstream")
          success(upstream, fulfilledUpstream, paymentSent)
          recordRelayDuration(startedAt, isSuccess = true)
          stopping()
        case WrappedPaymentFailed(PaymentFailed(_, _, failures, _)) =>
          context.log.debug(s"trampoline payment failed downstream")
          if (!fulfilledUpstream) {
            rejectPayment(upstream, translateError(nodeParams, failures, upstream, nextPayload))
          }
          recordRelayDuration(startedAt, isSuccess = fulfilledUpstream)
          stopping()
      }
    }

  /**
   * Once the downstream payment is settled (fulfilled or failed), we reject new upstream payments while we wait for our parent to stop us.
   */
  private def stopping(): Behavior[Command] = {
    parent ! NodeRelayer.RelayComplete(context.self, paymentHash, paymentSecret)
    Behaviors.receiveMessagePartial {
      rejectExtraHtlcPartialFunction orElse {
        case Stop => Behaviors.stopped
      }
    }
  }

  private def rejectExtraHtlcPartialFunction: PartialFunction[Command, Behavior[Command]] = {
    case Relay(nodeRelayPacket, _) =>
      rejectExtraHtlc(nodeRelayPacket.add)
      Behaviors.same
    // NB: this message would be sent from the payment FSM which we stopped before going to this state, but all this is asynchronous.
    // We always fail extraneous HTLCs. They are a spec violation from the sender, but harmless in the relay case.
    // By failing them fast (before the payment has reached the final recipient) there's a good chance the sender won't lose any money.
    // We don't expect to relay pay-to-open payments.
    case WrappedMultiPartExtraPaymentReceived(extraPaymentReceived) =>
      rejectExtraHtlc(extraPaymentReceived.payment.htlc)
      Behaviors.same
  }

  private def rejectExtraHtlc(add: UpdateAddHtlc): Unit = {
    context.log.warn("rejecting extra htlc #{} from channel {}", add.id, add.channelId)
    rejectHtlc(add.id, add.channelId, add.amountMsat)
  }

  private def rejectHtlc(htlcId: Long, channelId: ByteVector32, amount: MilliSatoshi, failure: Option[FailureMessage] = None): Unit = {
    val failureMessage = failure.getOrElse(IncorrectOrUnknownPaymentDetails(amount, nodeParams.currentBlockHeight))
    val cmd = CMD_FAIL_HTLC(htlcId, Right(failureMessage), commit = true)
    PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, channelId, cmd)
  }

  private def rejectPayment(upstream: Upstream.Hot.Trampoline, failure: Option[FailureMessage]): Unit = {
    Metrics.recordPaymentRelayFailed(failure.map(_.getClass.getSimpleName).getOrElse("Unknown"), Tags.RelayType.Trampoline)
    upstream.received.foreach(r => rejectHtlc(r.add.id, r.add.channelId, upstream.amountIn, failure))
  }

  private def fulfillPayment(upstream: Upstream.Hot.Trampoline, paymentPreimage: ByteVector32): Unit = upstream.received.foreach(r => {
    val cmd = CMD_FULFILL_HTLC(r.add.id, paymentPreimage, commit = true)
    PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, r.add.channelId, cmd)
  })

  private def success(upstream: Upstream.Hot.Trampoline, fulfilledUpstream: Boolean, paymentSent: PaymentSent): Unit = {
    // We may have already fulfilled upstream, but we can now emit an accurate relayed event and clean-up resources.
    if (!fulfilledUpstream) {
      fulfillPayment(upstream, paymentSent.paymentPreimage)
    }
    val incoming = upstream.received.map(r => PaymentRelayed.IncomingPart(r.add.amountMsat, r.add.channelId, r.receivedAt))
    val outgoing = paymentSent.parts.map(part => PaymentRelayed.OutgoingPart(part.amountWithFees, part.toChannelId, part.timestamp))
    context.system.eventStream ! EventStream.Publish(TrampolinePaymentRelayed(paymentHash, incoming, outgoing, paymentSent.recipientNodeId, paymentSent.recipientAmount))
  }

  private def recordRelayDuration(startedAt: TimestampMilli, isSuccess: Boolean): Unit =
    Metrics.RelayedPaymentDuration
      .withTag(Tags.Relay, Tags.RelayType.Trampoline)
      .withTag(Tags.Success, isSuccess)
      .record((TimestampMilli.now() - startedAt).toMillis, TimeUnit.MILLISECONDS)
}
