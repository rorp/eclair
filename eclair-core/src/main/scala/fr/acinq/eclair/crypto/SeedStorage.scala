package fr.acinq.eclair.crypto

import java.io.File

import scodec.bits.ByteVector

trait SeedStorage {
  def loadSeed(): ByteVector
}

class FileSeedStorage(datadir: File) extends SeedStorage {

  import java.nio.file.Files

  override def loadSeed(): ByteVector = {
    val seedPath = new File(datadir, "seed.dat")
    if (seedPath.exists()) {
      val seed = ByteVector(Files.readAllBytes(seedPath.toPath))
      if (seed.size != 32) throw new RuntimeException(s"Invalid seed size: ${seed.size}")
      seed
    } else {
      datadir.mkdirs()
      val seed = fr.acinq.eclair.randomBytes32
      Files.write(seedPath.toPath, seed.toArray)
      seed
    }
  }
}

class AWSSecretsManagerSeedStorage(secretId: String, seedKeyName: String, region_opt: Option[String]) extends SeedStorage {

  import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
  import com.amazonaws.services.secretsmanager.model._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.{DefaultFormats, _}

  implicit val formats = DefaultFormats

  private val client = {
    val builder = AWSSecretsManagerClientBuilder.standard()
    region_opt
      .map(region => builder.withRegion(region))
      .getOrElse(builder)
      .build()
  }

  override def loadSeed(): ByteVector = {
    val getRequest = new GetSecretValueRequest().withSecretId(secretId)

    val getResult = client.getSecretValue(getRequest)

    val secretString = getResult.getSecretString

    if (secretString == null) throw new RuntimeException("Cannot load secrets")
    val json = parse(secretString)
    val seedString = (json \ seedKeyName).extractOpt[String].getOrElse(throw new RuntimeException("Cannot find seed value"))

    val seed = ByteVector.fromHex(seedString).getOrElse(throw new RuntimeException("Seed value must be a valid hex string"))
    if (seed.size != 32) throw new RuntimeException(s"Invalid seed size: ${seed.size}")
    seed
  }
}
