/*
 * Copyright 2018 ACINQ SAS
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

package fr.acinq.eclair.io

import java.net.InetSocketAddress

import akka.actor.{Props, _}
import akka.event.Logging.MDC
import akka.io.Tcp.SO.KeepAlive
import akka.io.{IO, Tcp}
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.eclair.io.Client.ConnectionFailed
import fr.acinq.eclair.tor.Socks5Connection.{Socks5Connect, Socks5Connected}
import fr.acinq.eclair.tor.{Socks5Connection, Socks5ProxyParams}
import fr.acinq.eclair.wire.NodeAddress
import fr.acinq.eclair.{Logs, NodeParams}

import scala.concurrent.duration._

/**
  * Created by PM on 27/10/2015.
  *
  */
class Client(nodeParams: NodeParams, authenticator: ActorRef, remoteAddress: NodeAddress, remoteNodeId: PublicKey, origin_opt: Option[ActorRef]) extends Actor with DiagnosticActorLogging {

  import context.system

  // we could connect directly here but this allows to take advantage of the automated mdc configuration on message reception
  self ! 'connect

  def receive: Receive = {
    case 'connect =>
      val addressToConnect = nodeParams.socksProxy_opt.flatMap(proxyParams => Socks5ProxyParams.proxyAddress(remoteAddress, proxyParams)) match {
        case Some(proxyAddress) =>
          log.info(s"connecting to SOCKS5 proxy ${str(proxyAddress)}")
          proxyAddress
        case None =>
          val peerAddress = remoteAddress.socketAddress
          log.info(s"connecting to ${str(peerAddress)}")
          peerAddress
      }
      IO(Tcp) ! Tcp.Connect(addressToConnect, timeout = Some(50 seconds), options = KeepAlive(true) :: Nil, pullMode = true)
      context become connecting(addressToConnect)
  }

  def connecting(to: InetSocketAddress): Receive = {
    case Tcp.CommandFailed(_: Tcp.Connect) =>
      log.info(s"connection failed to ${str(to)}")
      origin_opt.map(_ ! Status.Failure(ConnectionFailed(remoteAddress)))
      context stop self

    case Tcp.Connected(peerOrProxyAddress, _) =>
      val connection = sender()
      context watch connection

      nodeParams.socksProxy_opt match {
        case Some(proxyParams) =>
          val proxyAddress = peerOrProxyAddress
          val peerAddress = remoteAddress.socketAddress
          log.info(s"connected to proxy ${str(proxyAddress)}")
          val proxy = context.actorOf(Socks5Connection.props(sender(), Socks5ProxyParams.proxyCredentials(proxyParams), Socks5Connect(peerAddress)))
          context become {
            case Tcp.CommandFailed(_: Socks5Connect) =>
              log.info(s"connection failed to ${str(peerAddress)} via SOCKS5 ${str(proxyAddress)}")
              origin_opt.map(_ ! Status.Failure(ConnectionFailed(remoteAddress)))
              context stop self
            case Socks5Connected(_) =>
              log.info(s"connected to ${str(peerAddress)} via SOCKS5 proxy ${str(proxyAddress)}")
              auth(proxy)
              context become connected(proxy)
          }
        case None =>
          log.info(s"connected to ${str(to)}")
          auth(connection)
          context become connected(connection)
      }
  }

  def connected(connection: ActorRef): Receive = {
    case Terminated(actor) if actor == connection =>
      context stop self
  }

  override def unhandled(message: Any): Unit = {
    log.warning(s"unhandled message=$message")
  }

  // we should not restart a failing socks client
  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) { case _ => SupervisorStrategy.Stop }

  override def mdc(currentMessage: Any): MDC = Logs.mdc(remoteNodeId_opt = Some(remoteNodeId))

  private def str(address: InetSocketAddress): String = s"${address.getHostString}:${address.getPort}"

  def auth(connection: ActorRef) = authenticator ! Authenticator.PendingAuth(connection, remoteNodeId_opt = Some(remoteNodeId), address = Authenticator.Outgoing(remoteAddress), origin_opt = origin_opt)
}

object Client {

  def props(nodeParams: NodeParams, authenticator: ActorRef, address: NodeAddress, remoteNodeId: PublicKey, origin_opt: Option[ActorRef]): Props = Props(new Client(nodeParams, authenticator, address, remoteNodeId, origin_opt))

  case class ConnectionFailed(address: NodeAddress) extends RuntimeException(s"connection failed to $address")
}
