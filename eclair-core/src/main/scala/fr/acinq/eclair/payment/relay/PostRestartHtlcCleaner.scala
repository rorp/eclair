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

package fr.acinq.eclair.payment.relay

import akka.Done
import akka.actor.{Actor, ActorRef, DiagnosticActorLogging, Props, Stash}
import akka.event.Logging.MDC
import akka.event.LoggingAdapter
import fr.acinq.bitcoin.scalacompat.ByteVector32
import fr.acinq.bitcoin.scalacompat.Crypto.PrivateKey
import fr.acinq.eclair.channel.Helpers.Closing
import fr.acinq.eclair.channel._
import fr.acinq.eclair.db._
import fr.acinq.eclair.payment.Monitoring.Tags
import fr.acinq.eclair.payment.{ChannelPaymentRelayed, IncomingPaymentPacket, PaymentFailed, PaymentSent}
import fr.acinq.eclair.transactions.DirectedHtlc.outgoing
import fr.acinq.eclair.wire.protocol._
import fr.acinq.eclair.{CustomCommitmentsPlugin, Feature, Features, Logs, MilliSatoshiLong, NodeParams, TimestampMilli}
import kamon.metric.{Gauge, Metric}

import scala.concurrent.Promise
import scala.util.Try

/**
 * Created by t-bast on 21/11/2019.
 */

/**
 * If we have stopped eclair while it was handling HTLCs, it is possible that we are in a state were incoming HTLCs were
 * committed by both sides, but we didn't have time to send and/or sign corresponding HTLCs to the downstream node.
 * It's also possible that we partially forwarded a payment (if MPP was used downstream): we have lost the intermediate
 * state necessary to retry that payment, so we need to wait for the partial HTLC set sent downstream to either fail or
 * fulfill (and forward the result upstream).
 *
 * If we were sending a payment (no downstream HTLCs) when we stopped eclair, we might have sent only a portion of the
 * payment (because of multi-part): we have lost the intermediate state necessary to retry that payment, so we need to
 * wait for the partial HTLC set sent downstream to either fail or fulfill the payment in our DB.
 */
class PostRestartHtlcCleaner(nodeParams: NodeParams, register: ActorRef, initialized: Option[Promise[Done]] = None) extends Actor with Stash with DiagnosticActorLogging {

  import PostRestartHtlcCleaner._

  // we pass these to helpers classes so that they have the logging context
  implicit def implicitLog: LoggingAdapter = log

  context.system.eventStream.subscribe(self, classOf[ChannelStateChanged])

  override def receive: Receive = {
    case init: Init =>
      // If we do nothing after a restart, incoming HTLCs that were committed upstream but not relayed will eventually
      // expire and we won't lose money, but the channel will get closed, which is a major inconvenience. We want to detect
      // this and fast-fail those HTLCs and thus preserve channels.
      //
      // Outgoing HTLC sets that are still pending may either succeed or fail: we need to watch them to properly forward the
      // result upstream to preserve channels.
      val brokenHtlcs: BrokenHtlcs = {
        val channels = listLocalChannels(init.channels)
        val onTheFlyPayments = nodeParams.db.liquidity.listPendingOnTheFlyPayments().values.flatten.toSet
        val nonStandardIncomingHtlcs: Seq[IncomingHtlc] = nodeParams.pluginParams.collect { case p: CustomCommitmentsPlugin => p.getIncomingHtlcs(nodeParams, log) }.flatten
        val htlcsIn: Seq[IncomingHtlc] = getIncomingHtlcs(channels, nodeParams.db.payments, nodeParams.privateKey, nodeParams.features) ++ nonStandardIncomingHtlcs
        val nonStandardRelayedOutHtlcs: Map[Origin.Cold, Set[(ByteVector32, Long)]] = nodeParams.pluginParams.collect { case p: CustomCommitmentsPlugin => p.getHtlcsRelayedOut(htlcsIn, nodeParams, log) }.flatten.toMap
        val relayedOut: Map[Origin.Cold, Set[(ByteVector32, Long)]] = getHtlcsRelayedOut(nodeParams, channels, htlcsIn) ++ nonStandardRelayedOutHtlcs

        val settledHtlcs: Set[(ByteVector32, Long)] = nodeParams.db.pendingCommands.listSettlementCommands().map { case (channelId, cmd) => (channelId, cmd.id) }.toSet
        val notRelayed = htlcsIn.filterNot(htlcIn => {
          // If an HTLC has been relayed and then settled downstream, it will not have a matching entry in relayedOut.
          // When that happens, there will be an HTLC settlement command in the pendingRelay DB, and we will let the channel
          // replay it instead of sending a conflicting command.
          relayedOut.keys.exists(origin => matchesOrigin(htlcIn.add, origin)) || settledHtlcs.contains((htlcIn.add.channelId, htlcIn.add.id))
        })
        cleanupRelayDb(htlcsIn, nodeParams.db.pendingCommands)

        log.info(s"htlcsIn=${htlcsIn.length} notRelayed=${notRelayed.length} relayedOut=${relayedOut.values.flatten.size}")
        log.info("notRelayed={}", notRelayed.map(htlc => (htlc.add.channelId, htlc.add.id)))
        log.info("relayedOut={}", relayedOut)
        BrokenHtlcs(notRelayed, relayedOut, Set.empty, onTheFlyPayments)
      }

      Metrics.PendingNotRelayed.update(brokenHtlcs.notRelayed.size)
      Metrics.PendingRelayedOut.update(brokenHtlcs.relayedOut.keySet.size)

      // Once we've loaded the channels and identified broken HTLCs, we let other components know they can proceed.
      Try(initialized.map(_.success(Done)))

      unstashAll()
      context.become(main(brokenHtlcs))

    case _ =>
      stash()
  }


  def main(brokenHtlcs: BrokenHtlcs): Receive = {
    // When channels are restarted we immediately fail the incoming HTLCs that weren't relayed.
    case e@ChannelStateChanged(channel, channelId, _, _, WAIT_FOR_INIT_INTERNAL | OFFLINE | SYNCING | CLOSING, NORMAL | SHUTDOWN | CLOSING | CLOSED, Some(commitments)) =>
      log.debug("channel {}: {} -> {}", channelId, e.previousState, e.currentState)
      val acked = brokenHtlcs.notRelayed
        .filter(_.add.channelId == channelId) // only consider htlcs coming from this channel
        .filter {
          case IncomingHtlc(htlc, preimage_opt) if commitments.getIncomingHtlcCrossSigned(htlc.id).isDefined =>
            // this htlc is cross signed in the current commitment, we can settle it
            preimage_opt match {
              case Some(preimage) =>
                Metrics.Resolved.withTag(Tags.Success, value = true).withTag(Metrics.Relayed, value = false).increment()
                if (e.currentState != CLOSED) {
                  log.info(s"fulfilling broken htlc=$htlc")
                  channel ! CMD_FULFILL_HTLC(htlc.id, preimage, None, commit = true)
                } else {
                  log.info(s"got preimage but upstream channel is closed for htlc=$htlc")
                }
              case None if brokenHtlcs.pendingPayments.contains(htlc.paymentHash) =>
                // We don't fail on-the-fly HTLCs that have been funded: we haven't been paid our fee yet, so we will
                // retry relaying them unless we reach the HTLC timeout.
                log.info("htlc #{} from channelId={} wasn't relayed, but has a pending on-the-fly relay (paymentHash={})", htlc.id, htlc.channelId, htlc.paymentHash)
              case None =>
                Metrics.Resolved.withTag(Tags.Success, value = false).withTag(Metrics.Relayed, value = false).increment()
                if (e.currentState != CLOSING && e.currentState != CLOSED) {
                  log.info(s"failing not relayed htlc=$htlc")
                  val cmd = htlc.pathKey_opt match {
                    case Some(_) =>
                      // The incoming HTLC contains a path key: we must be an intermediate node in a blinded path,
                      // and we thus need to return an update_fail_malformed_htlc.
                      val failure = InvalidOnionBlinding(ByteVector32.Zeroes)
                      CMD_FAIL_MALFORMED_HTLC(htlc.id, failure.onionHash, failure.code, commit = true)
                    case None =>
                      CMD_FAIL_HTLC(htlc.id, FailureReason.LocalFailure(TemporaryNodeFailure()), None, commit = true)
                  }
                  channel ! cmd
                } else {
                  log.info(s"would fail but upstream channel is closed for htlc=$htlc")
                }
            }
            false // the channel may very well be disconnected before we sign (=ack) the fail/fulfill, so we keep it for now
          case _ =>
            true // the htlc has already been settled, we can forget about it now
        }
      acked.foreach(htlc => log.info(s"forgetting htlc id=${htlc.add.id} channelId=${htlc.add.channelId}"))
      val notRelayed1 = brokenHtlcs.notRelayed diff acked
      Metrics.PendingNotRelayed.update(notRelayed1.size)
      context become main(brokenHtlcs.copy(notRelayed = notRelayed1))

    case _: ChannelStateChanged => // ignore other channel state changes

    case RES_ADD_SETTLED(o: Origin.Cold, htlc, fulfill: HtlcResult.Fulfill) =>
      log.info("htlc #{} from channelId={} fulfilled downstream", htlc.id, htlc.channelId)
      handleDownstreamFulfill(brokenHtlcs, o, htlc, fulfill.paymentPreimage)

    case RES_ADD_SETTLED(o: Origin.Cold, htlc, fail: HtlcResult.Fail) =>
      if (htlc.fundingFee_opt.nonEmpty) {
        log.info("htlc #{} from channelId={} failed downstream but has a pending on-the-fly funding", htlc.id, htlc.channelId)
        // We don't fail upstream: we haven't been paid our funding fee yet, so we will try relaying again.
      } else {
        log.info("htlc #{} from channelId={} failed downstream: {}", htlc.id, htlc.channelId, fail.getClass.getSimpleName)
        handleDownstreamFailure(brokenHtlcs, o, htlc, fail)
      }

    case GetBrokenHtlcs => sender() ! brokenHtlcs
  }

  private def handleDownstreamFulfill(brokenHtlcs: BrokenHtlcs, origin: Origin.Cold, fulfilledHtlc: UpdateAddHtlc, paymentPreimage: ByteVector32): Unit =
    brokenHtlcs.relayedOut.get(origin) match {
      case Some(relayedOut) => origin.upstream match {
        case Upstream.Local(id) =>
          val feesPaid = 0.msat // fees are unknown since we lost the reference to the payment
          nodeParams.db.payments.getOutgoingPayment(id) match {
            case Some(p) =>
              nodeParams.db.payments.updateOutgoingPayment(PaymentSent(p.parentId, fulfilledHtlc.paymentHash, paymentPreimage, p.recipientAmount, p.recipientNodeId, PaymentSent.PartialPayment(id, fulfilledHtlc.amountMsat, feesPaid, fulfilledHtlc.channelId, None) :: Nil, None))
              // If all downstream HTLCs are now resolved, we can emit the payment event.
              val payments = nodeParams.db.payments.listOutgoingPayments(p.parentId)
              if (!payments.exists(p => p.status == OutgoingPaymentStatus.Pending)) {
                val succeeded = payments.collect {
                  case OutgoingPayment(id, _, _, _, _, amount, _, _, _, _, _, OutgoingPaymentStatus.Succeeded(_, feesPaid, _, completedAt)) =>
                    PaymentSent.PartialPayment(id, amount, feesPaid, ByteVector32.Zeroes, None, completedAt)
                }
                val sent = PaymentSent(p.parentId, fulfilledHtlc.paymentHash, paymentPreimage, p.recipientAmount, p.recipientNodeId, succeeded, None)
                log.info(s"payment id=${sent.id} paymentHash=${sent.paymentHash} successfully sent (amount=${sent.recipientAmount})")
                context.system.eventStream.publish(sent)
              }
            case None =>
              log.warning(s"database inconsistency detected: payment $id is fulfilled but doesn't have a corresponding database entry")
              // Since we don't have a matching DB entry, we've lost the payment recipient and total amount, so we put
              // dummy values in the DB (to make sure we store the preimage) but we don't emit an event.
              val dummyFinalAmount = fulfilledHtlc.amountMsat
              val dummyNodeId = nodeParams.nodeId
              nodeParams.db.payments.addOutgoingPayment(OutgoingPayment(id, id, None, fulfilledHtlc.paymentHash, PaymentType.Standard, fulfilledHtlc.amountMsat, dummyFinalAmount, dummyNodeId, TimestampMilli.now(), None, None, OutgoingPaymentStatus.Pending))
              nodeParams.db.payments.updateOutgoingPayment(PaymentSent(id, fulfilledHtlc.paymentHash, paymentPreimage, dummyFinalAmount, dummyNodeId, PaymentSent.PartialPayment(id, fulfilledHtlc.amountMsat, feesPaid, fulfilledHtlc.channelId, None) :: Nil, None))
          }
          // There can never be more than one pending downstream HTLC for a given local origin (a multi-part payment is
          // instead spread across multiple local origins) so we can now forget this origin.
          Metrics.PendingRelayedOut.decrement()
          context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut - origin))
        case Upstream.Cold.Channel(originChannelId, originHtlcId, amountIn) =>
          log.info(s"received preimage for paymentHash=${fulfilledHtlc.paymentHash}: fulfilling 1 HTLC upstream")
          if (relayedOut != Set((fulfilledHtlc.channelId, fulfilledHtlc.id))) {
            log.error(s"unexpected channel relay downstream HTLCs: expected (${fulfilledHtlc.channelId},${fulfilledHtlc.id}), found $relayedOut")
          }
          PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, originChannelId, CMD_FULFILL_HTLC(originHtlcId, paymentPreimage, None, commit = true))
          // We don't know when we received this HTLC so we just pretend that we received it just now.
          context.system.eventStream.publish(ChannelPaymentRelayed(amountIn, fulfilledHtlc.amountMsat, fulfilledHtlc.paymentHash, originChannelId, fulfilledHtlc.channelId, TimestampMilli.now(), TimestampMilli.now()))
          Metrics.PendingRelayedOut.decrement()
          context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut - origin))
        case Upstream.Cold.Trampoline(originHtlcs) =>
          // We fulfill upstream as soon as we have the payment preimage available.
          if (!brokenHtlcs.settledUpstream.contains(origin)) {
            log.info(s"received preimage for paymentHash=${fulfilledHtlc.paymentHash}: fulfilling ${originHtlcs.length} HTLCs upstream")
            originHtlcs.foreach { case Upstream.Cold.Channel(channelId, htlcId, _) =>
              Metrics.Resolved.withTag(Tags.Success, value = true).withTag(Metrics.Relayed, value = true).increment()
              PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, channelId, CMD_FULFILL_HTLC(htlcId, paymentPreimage, None, commit = true))
            }
          }
          val relayedOut1 = relayedOut diff Set((fulfilledHtlc.channelId, fulfilledHtlc.id))
          if (relayedOut1.isEmpty) {
            log.info(s"payment with paymentHash=${fulfilledHtlc.paymentHash} successfully relayed")
            // We could emit a TrampolinePaymentRelayed event but that requires more book-keeping on incoming HTLCs.
            // It seems low priority so isn't done at the moment but can be added when we feel we need it.
            Metrics.PendingRelayedOut.decrement()
            context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut - origin, settledUpstream = brokenHtlcs.settledUpstream - origin))
          } else {
            context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut + (origin -> relayedOut1), settledUpstream = brokenHtlcs.settledUpstream + origin))
          }
      }
      case None =>
        Metrics.Unhandled.withTag(Metrics.Hint, "MissingOrigin").increment()
        log.error(s"received fulfill with unknown origin $origin for htlcId=${fulfilledHtlc.id}, channelId=${fulfilledHtlc.channelId}: cannot forward upstream")
    }

  private def handleDownstreamFailure(brokenHtlcs: BrokenHtlcs, origin: Origin.Cold, failedHtlc: UpdateAddHtlc, fail: HtlcResult.Fail): Unit =
    brokenHtlcs.relayedOut.get(origin) match {
      case Some(relayedOut) =>
        // If this is a local payment, we need to update the DB:
        origin.upstream match {
          case Upstream.Local(id) => nodeParams.db.payments.updateOutgoingPayment(PaymentFailed(id, failedHtlc.paymentHash, Nil))
          case _ =>
        }
        val relayedOut1 = relayedOut diff Set((failedHtlc.channelId, failedHtlc.id))
        // This was the last downstream HTLC we were waiting for.
        if (relayedOut1.isEmpty) {
          // If we haven't already settled upstream, we can fail now.
          if (!brokenHtlcs.settledUpstream.contains(origin)) {
            origin.upstream match {
              case Upstream.Local(id) => nodeParams.db.payments.getOutgoingPayment(id).foreach(p => {
                val payments = nodeParams.db.payments.listOutgoingPayments(p.parentId)
                if (payments.forall(_.status.isInstanceOf[OutgoingPaymentStatus.Failed])) {
                  log.warning(s"payment failed for paymentHash=${failedHtlc.paymentHash}")
                  context.system.eventStream.publish(PaymentFailed(p.parentId, failedHtlc.paymentHash, Nil))
                }
              })
              case Upstream.Cold.Channel(originChannelId, originHtlcId, _) =>
                log.warning(s"payment failed for paymentHash=${failedHtlc.paymentHash}: failing 1 HTLC upstream")
                Metrics.Resolved.withTag(Tags.Success, value = false).withTag(Metrics.Relayed, value = true).increment()
                val cmd = failedHtlc.pathKey_opt match {
                  case Some(_) =>
                    // If we are inside a blinded path, we cannot know whether we're the introduction node or not since
                    // we don't have access to the incoming onion: to avoid leaking information, we act as if we were an
                    // intermediate node and send invalid_onion_blinding in an update_fail_malformed_htlc message.
                    val failure = InvalidOnionBlinding(ByteVector32.Zeroes)
                    CMD_FAIL_MALFORMED_HTLC(originHtlcId, failure.onionHash, failure.code, commit = true)
                  case None =>
                    ChannelRelay.translateRelayFailure(originHtlcId, fail, None)
                }
                PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, originChannelId, cmd)
              case Upstream.Cold.Trampoline(originHtlcs) =>
                log.warning(s"payment failed for paymentHash=${failedHtlc.paymentHash}: failing ${originHtlcs.length} HTLCs upstream")
                originHtlcs.foreach { case Upstream.Cold.Channel(channelId, htlcId, _) =>
                  Metrics.Resolved.withTag(Tags.Success, value = false).withTag(Metrics.Relayed, value = true).increment()
                  // We don't bother decrypting the downstream failure to forward a more meaningful error upstream, it's
                  // very likely that it won't be actionable anyway because of our node restart.
                  PendingCommandsDb.safeSend(register, nodeParams.db.pendingCommands, channelId, CMD_FAIL_HTLC(htlcId, FailureReason.LocalFailure(TemporaryNodeFailure()), None, commit = true))
                }
            }
          }
          // We can forget about this payment since it has been fully settled downstream and upstream.
          Metrics.PendingRelayedOut.decrement()
          context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut - origin, settledUpstream = brokenHtlcs.settledUpstream - origin))
        } else {
          context become main(brokenHtlcs.copy(relayedOut = brokenHtlcs.relayedOut + (origin -> relayedOut1)))
        }
      case None =>
        Metrics.Unhandled.withTag(Metrics.Hint, "MissingOrigin").increment()
        log.error(s"received failure with unknown origin $origin for htlcId=${failedHtlc.id}, channelId=${failedHtlc.channelId}")
    }

  override def mdc(currentMessage: Any): MDC = {
    val (remoteNodeId_opt, channelId_opt) = currentMessage match {
      case e: ChannelStateChanged => (Some(e.remoteNodeId), Some(e.channelId))
      case e: RES_ADD_SETTLED[_, _] => (None, Some(e.htlc.channelId))
      case _ => (None, None)
    }
    Logs.mdc(remoteNodeId_opt = remoteNodeId_opt, channelId_opt = channelId_opt, nodeAlias_opt = Some(nodeParams.alias))
  }

}

object PostRestartHtlcCleaner {

  def props(nodeParams: NodeParams, register: ActorRef, initialized: Option[Promise[Done]] = None): Props = Props(new PostRestartHtlcCleaner(nodeParams, register, initialized))

  case class Init(channels: Seq[PersistentChannelData])

  case object GetBrokenHtlcs

  object Metrics {

    import kamon.Kamon

    val Relayed = "relayed"
    val Hint = "hint"

    private val pending = Kamon.gauge("payment.broken-htlcs.pending", "Broken HTLCs because of a node restart")
    val PendingNotRelayed: Gauge = pending.withTag(Relayed, value = false)
    val PendingRelayedOut: Gauge = pending.withTag(Relayed, value = true)
    val Resolved: Metric.Gauge = Kamon.gauge("payment.broken-htlcs.resolved", "Broken HTLCs resolved after a node restart")
    val Unhandled: Metric.Gauge = Kamon.gauge("payment.broken-htlcs.unhandled", "Broken HTLCs that we don't know how to handle")

  }

  /**
   * @param add          incoming HTLC that was committed upstream.
   * @param preimage_opt payment preimage if the payment succeeded downstream.
   */
  case class IncomingHtlc(add: UpdateAddHtlc, preimage_opt: Option[ByteVector32])

  /**
   * Payments that may be in a broken state after a restart.
   *
   * @param notRelayed      incoming HTLCs that were committed upstream but not relayed downstream.
   * @param relayedOut      outgoing HTLC sets that may have been incompletely sent and need to be watched.
   * @param settledUpstream upstream payments that have already been settled (failed or fulfilled) by this actor.
   * @param pendingPayments payments that are pending and will be relayed: we mustn't fail them upstream.
   */
  case class BrokenHtlcs(notRelayed: Seq[IncomingHtlc], relayedOut: Map[Origin.Cold, Set[(ByteVector32, Long)]], settledUpstream: Set[Origin.Cold], pendingPayments: Set[ByteVector32])

  /** Returns true if the given HTLC matches the given origin. */
  private def matchesOrigin(htlcIn: UpdateAddHtlc, origin: Origin.Cold): Boolean = origin.upstream match {
    case _: Upstream.Local => false
    case o: Upstream.Cold.Channel => o.originChannelId == htlcIn.channelId && o.originHtlcId == htlcIn.id
    case o: Upstream.Cold.Trampoline => o.originHtlcs.exists(h => h.originChannelId == htlcIn.channelId && h.originHtlcId == htlcIn.id)
  }

  /**
   * When we restart while we're receiving a payment, we need to look at the DB to find out whether the payment
   * succeeded or not (which may have triggered external downstream components to treat the payment as received and
   * ship some physical goods to a customer).
   */
  private def shouldFulfill(finalPacket: IncomingPaymentPacket.FinalPacket, paymentsDb: IncomingPaymentsDb): Option[ByteVector32] =
    paymentsDb.getIncomingPayment(finalPacket.add.paymentHash).flatMap(p => p.status match {
      case _: IncomingPaymentStatus.Received => Some(p.paymentPreimage)
      case _ => None
    })

  private def decryptedIncomingHtlcs(paymentsDb: IncomingPaymentsDb): PartialFunction[Either[FailureMessage, IncomingPaymentPacket], IncomingHtlc] = {
    // When we're not the final recipient, we'll only consider HTLCs that aren't relayed downstream, so no need to look for a preimage.
    case Right(p: IncomingPaymentPacket.ChannelRelayPacket) => IncomingHtlc(p.add, None)
    case Right(p: IncomingPaymentPacket.NodeRelayPacket) => IncomingHtlc(p.add, None)
    // When we're the final recipient, we want to know if we want to fulfill or fail.
    case Right(p: IncomingPaymentPacket.FinalPacket) => IncomingHtlc(p.add, shouldFulfill(p, paymentsDb))
  }

  /** @return incoming HTLCs that have been *cross-signed* (that potentially have been relayed). */
  private def getIncomingHtlcs(channels: Seq[PersistentChannelData], paymentsDb: IncomingPaymentsDb, privateKey: PrivateKey, features: Features[Feature]): Seq[IncomingHtlc] = {
    // We are interested in incoming HTLCs, that have been *cross-signed* (otherwise they wouldn't have been relayed).
    // They signed it first, so the HTLC will first appear in our commitment tx, and later on in their commitment when
    // we subsequently sign it. That's why we need to look in *their* commitment with direction=OUT.
    channels
      .collect { case c: ChannelDataWithCommitments => c }
      .flatMap(_.commitments.latest.remoteCommit.spec.htlcs)
      .collect(outgoing)
      .map(IncomingPaymentPacket.decrypt(_, privateKey, features))
      .collect(decryptedIncomingHtlcs(paymentsDb))
  }

  /** @return whether a given HTLC is a pending incoming HTLC. */
  private def isPendingUpstream(channelId: ByteVector32, htlcId: Long, htlcsIn: Seq[IncomingHtlc]): Boolean =
    htlcsIn.exists(htlc => htlc.add.channelId == channelId && htlc.add.id == htlcId)

  private def groupByOrigin(htlcsOut: Seq[(Origin.Cold, ByteVector32, Long)], htlcsIn: Seq[IncomingHtlc]): Map[Origin.Cold, Set[(ByteVector32, Long)]] =
    htlcsOut
      .groupBy { case (origin, _, _) => origin }
      .view
      .mapValues(_.map { case (_, channelId, htlcId) => (channelId, htlcId) }.toSet)
      // We are only interested in HTLCs that are pending upstream (not fulfilled nor failed yet).
      // It may be the case that we have unresolved HTLCs downstream that have been resolved upstream when the downstream
      // channel is closing (e.g. due to an HTLC timeout) because cooperatively failing the HTLC downstream will be
      // instant whereas the uncooperative close of the downstream channel will take time.
      .filterKeys {
        case Origin.Cold(_: Upstream.Local) => true
        case Origin.Cold(o: Upstream.Cold.Channel) => isPendingUpstream(o.originChannelId, o.originHtlcId, htlcsIn)
        case Origin.Cold(o: Upstream.Cold.Trampoline) => o.originHtlcs.exists(h => isPendingUpstream(h.originChannelId, h.originHtlcId, htlcsIn))
      }
      .toMap

  /** @return pending outgoing HTLCs, grouped by their upstream origin. */
  private def getHtlcsRelayedOut(nodeParams: NodeParams, channels: Seq[PersistentChannelData], htlcsIn: Seq[IncomingHtlc])(implicit log: LoggingAdapter): Map[Origin.Cold, Set[(ByteVector32, Long)]] = {
    val htlcsOut = channels
      .collect { case c: ChannelDataWithCommitments => c }
      .flatMap { c =>
        // Filter out HTLCs that will never reach the blockchain or have already been settled on-chain.
        val htlcsToIgnore: Set[Long] = c match {
          case d: DATA_CLOSING =>
            val closingType_opt = Closing.isClosingTypeAlreadyKnown(d)
            val overriddenHtlcs: Set[Long] = (closingType_opt match {
              case Some(c: Closing.LocalClose) => Closing.overriddenOutgoingHtlcs(d, c.localCommitPublished.commitTx)
              case Some(c: Closing.RemoteClose) => Closing.overriddenOutgoingHtlcs(d, c.remoteCommitPublished.commitTx)
              case Some(c: Closing.RevokedClose) => Closing.overriddenOutgoingHtlcs(d, c.revokedCommitPublished.commitTx)
              case Some(c: Closing.RecoveryClose) => Closing.overriddenOutgoingHtlcs(d, c.remoteCommitPublished.commitTx)
              case Some(_: Closing.MutualClose) => Set.empty[UpdateAddHtlc]
              case None => Set.empty[UpdateAddHtlc]
            }).map(_.id)
            val confirmedTxs = closingType_opt match {
              case Some(c: Closing.LocalClose) => c.localCommitPublished.irrevocablySpent.values.toSet
              case Some(c: Closing.RemoteClose) => c.remoteCommitPublished.irrevocablySpent.values.toSet
              case Some(c: Closing.RevokedClose) => c.revokedCommitPublished.irrevocablySpent.values.toSet
              case Some(c: Closing.RecoveryClose) => c.remoteCommitPublished.irrevocablySpent.values.toSet
              case Some(_: Closing.MutualClose) => Set.empty
              case None => Set.empty
            }
            val channelKeys = nodeParams.channelKeyManager.channelKeys(d.commitments.channelParams.channelConfig, d.commitments.localChannelParams.fundingKeyPath)
            val timedOutHtlcs: Set[Long] = (closingType_opt match {
              case Some(c: Closing.LocalClose) => confirmedTxs.flatMap(tx => Closing.trimmedOrTimedOutHtlcs(channelKeys, d.commitments.latest, c.localCommit, tx))
              case Some(c: Closing.RemoteClose) => confirmedTxs.flatMap(tx => Closing.trimmedOrTimedOutHtlcs(channelKeys, d.commitments.latest, c.remoteCommit, tx))
              case Some(_: Closing.RevokedClose) => Set.empty // revoked commitments are handled using [[overriddenOutgoingHtlcs]] above
              case Some(_: Closing.RecoveryClose) => Set.empty // we lose htlc outputs in dataloss protection scenarios (future remote commit)
              case Some(_: Closing.MutualClose) => Set.empty
              case None => Set.empty
            }).map(_.id)
            overriddenHtlcs ++ timedOutHtlcs
          case _ => Set.empty
        }
        c.commitments.originChannels.collect {
          case (outgoingHtlcId, origin: Origin.Cold) if !htlcsToIgnore.contains(outgoingHtlcId) => (origin, c.channelId, outgoingHtlcId)
        }
      }
    groupByOrigin(htlcsOut, htlcsIn)
  }

  /**
   * List local channels that may have pending HTLCs, ignoring channels that are still in CLOSING state but have
   * actually been closed. This can happen when the app is stopped just after a channel state has transitioned to CLOSED
   * and before it has effectively been removed. Such closed channels will automatically be removed once the channel is
   * restored.
   */
  private def listLocalChannels(channels: Seq[PersistentChannelData]): Seq[PersistentChannelData] =
    channels.filterNot(c => Closing.isClosed(c, None).isDefined)

  /**
   * We store [[CMD_FULFILL_HTLC]]/[[CMD_FAIL_HTLC]]/[[CMD_FAIL_MALFORMED_HTLC]] in a database
   * (see [[fr.acinq.eclair.db.PendingCommandsDb]]) because we don't want to lose preimages, or to forget to fail
   * incoming htlcs, which would lead to unwanted channel closings.
   *
   * Because of the way our watcher works, in a scenario where a downstream channel has gone to the blockchain, it may
   * send several times the same command, and the upstream channel may have disappeared in the meantime.
   *
   * That's why we need to periodically clean up the pending relay db.
   */
  private def cleanupRelayDb(htlcsIn: Seq[IncomingHtlc], relayDb: PendingCommandsDb)(implicit log: LoggingAdapter): Unit = {
    // We are interested in incoming HTLCs, that have been *cross-signed* (otherwise they wouldn't have been relayed).
    // If the HTLC is not in their commitment, it means that we have already fulfilled/failed it and that we can remove
    // the command from the pending relay db.
    val channel2Htlc: Seq[(ByteVector32, Long)] = htlcsIn.map { case IncomingHtlc(add, _) => (add.channelId, add.id) }
    val pendingRelay: Set[(ByteVector32, Long)] = relayDb.listSettlementCommands().map { case (channelId, cmd) => (channelId, cmd.id) }.toSet
    val toClean = pendingRelay -- channel2Htlc
    toClean.foreach {
      case (channelId, htlcId) =>
        log.info(s"cleaning up channelId=$channelId htlcId=$htlcId from relay db")
        relayDb.removeSettlementCommand(channelId, htlcId)
    }
  }

}