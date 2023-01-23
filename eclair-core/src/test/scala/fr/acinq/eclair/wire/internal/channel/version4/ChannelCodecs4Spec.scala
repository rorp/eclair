package fr.acinq.eclair.wire.internal.channel.version4

import com.softwaremill.quicklens.ModifyPimp
import fr.acinq.bitcoin.scalacompat.Satoshi
import fr.acinq.eclair.FeatureSupport.{Mandatory, Optional}
import fr.acinq.eclair.Features.{ChannelRangeQueries, PaymentSecret, VariableLengthOnion}
import fr.acinq.eclair.channel._
import fr.acinq.eclair.wire.internal.channel.ChannelCodecsSpec.normal
import fr.acinq.eclair.wire.internal.channel.version4.ChannelCodecs4.Codecs.{channelConfigCodec, remoteParamsCodec}
import fr.acinq.eclair.wire.internal.channel.version4.ChannelCodecs4.channelDataCodec
import fr.acinq.eclair.{CltvExpiryDelta, Features, MilliSatoshi, RealShortChannelId, ShortChannelId, UInt64, randomKey}
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits._

class ChannelCodecs4Spec extends AnyFunSuite {

  test("basic serialization test (NORMAL)") {
    val data = normal
    val bin = channelDataCodec.encode(data).require
    val check = channelDataCodec.decodeValue(bin).require
    assert(data.commitments.localCommit.spec == check.commitments.localCommit.spec)
    assert(data == check)
  }

  test("encode/decode channel configuration options") {
    assert(channelConfigCodec.encode(ChannelConfig(Set.empty[ChannelConfigOption])).require.bytes == hex"00")
    assert(channelConfigCodec.decode(hex"00".bits).require.value == ChannelConfig(Set.empty[ChannelConfigOption]))
    assert(channelConfigCodec.decode(hex"01f0".bits).require.value == ChannelConfig(Set.empty[ChannelConfigOption]))
    assert(channelConfigCodec.decode(hex"020000".bits).require.value == ChannelConfig(Set.empty[ChannelConfigOption]))

    assert(channelConfigCodec.encode(ChannelConfig.standard).require.bytes == hex"0101")
    assert(channelConfigCodec.encode(ChannelConfig(ChannelConfig.FundingPubKeyBasedChannelKeyPath)).require.bytes == hex"0101")
    assert(channelConfigCodec.decode(hex"0101".bits).require.value == ChannelConfig(ChannelConfig.FundingPubKeyBasedChannelKeyPath))
    assert(channelConfigCodec.decode(hex"01ff".bits).require.value == ChannelConfig(ChannelConfig.FundingPubKeyBasedChannelKeyPath))
    assert(channelConfigCodec.decode(hex"020001".bits).require.value == ChannelConfig(ChannelConfig.FundingPubKeyBasedChannelKeyPath))
  }

  test("encode/decode optional shutdown script") {
    val codec = remoteParamsCodec(ChannelFeatures())
    val remoteParams = RemoteParams(
      randomKey().publicKey,
      Satoshi(600),
      UInt64(123456L),
      Some(Satoshi(300)),
      MilliSatoshi(1000),
      CltvExpiryDelta(42),
      42,
      randomKey().publicKey,
      randomKey().publicKey,
      randomKey().publicKey,
      randomKey().publicKey,
      randomKey().publicKey,
      Features(ChannelRangeQueries -> Optional, VariableLengthOnion -> Mandatory, PaymentSecret -> Mandatory),
      None)
    assert(codec.decodeValue(codec.encode(remoteParams).require).require == remoteParams)
    val remoteParams1 = remoteParams.copy(shutdownScript = Some(ByteVector.fromValidHex("deadbeef")))
    assert(codec.decodeValue(codec.encode(remoteParams1).require).require == remoteParams1)

    val dataWithoutRemoteShutdownScript = normal.modify(_.metaCommitments.params.remoteParams).setTo(remoteParams)
    assert(channelDataCodec.decode(channelDataCodec.encode(dataWithoutRemoteShutdownScript).require).require.value == dataWithoutRemoteShutdownScript)

    val dataWithRemoteShutdownScript = normal.modify(_.metaCommitments.params.remoteParams).setTo(remoteParams1)
    assert(channelDataCodec.decode(channelDataCodec.encode(dataWithRemoteShutdownScript).require).require.value == dataWithRemoteShutdownScript)
  }

}