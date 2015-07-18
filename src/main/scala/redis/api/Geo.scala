package redis.api.geo

import akka.util.ByteString
import redis._
import redis.api.Order
import redis.protocol.MultiBulk

case class Geoadd[K, V](key: K, geoMembers: Seq[(Double, Double, V)])(implicit keySeria: ByteStringSerializer[K], convert: ByteStringSerializer[V])
  extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("GEOADD", keySeria.serialize(key) +: geoMembers.foldLeft(Seq.empty[ByteString])({
    case (acc, e) => ByteString(e._1.toString) +: ByteString(e._2.toString) +: convert.serialize(e._3) +: acc
  }))
}

case class Geodist[K, V](key: K, member1: V, member2: V, unit: String)(implicit keySeria: ByteStringSerializer[K], convert: ByteStringSerializer[V])
  extends RedisCommandBulkOptionDouble {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("GEODIST", Seq(keySeria.serialize(key), convert.serialize(member1), convert.serialize(member2), ByteString(unit)))
}

case class Geohash[K, V](key: K, members: Seq[V])(implicit keySeria: ByteStringSerializer[K], convert: ByteStringSerializer[V])
  extends RedisCommandMultiBulk[Seq[Option[String]]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("GEOHASH", keySeria.serialize(key) +: members.map(convert.serialize))

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toSeqOptionByteString[String](mb)
}

case class Geopos[K, V](key: K, members: Seq[V])(implicit keySeria: ByteStringSerializer[K], convert: ByteStringSerializer[V])
  extends RedisCommandMultiBulk[Seq[Option[(Double, Double)]]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("GEOPOS", keySeria.serialize(key) +: members.map(convert.serialize))

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toNestedSeqByteString[Double](mb).map {
    sub => {
      sub.toList match {
        case x :: y :: nil => Some((x, y))
        case _ => None
      }
    }
  }
}

case class GeoradiusWithdist[K, R](key: K, longitude: Double, latitude: Double, radius: Double, unit: String, order: Option[Order])(implicit keySeria: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R])
    extends RedisCommandMultiBulk[Seq[(R, Double)]] {
    val isMasterOnly = false
    val encodedRequest: ByteString = encode("GEORADIUS",
      Seq(keySeria.serialize(key), ByteString(longitude.toString), ByteString(latitude.toString), ByteString(radius.toString), ByteString(unit), ByteString("WITHDIST")) ++ order.map(ord=>Seq(ByteString(ord.toString))).getOrElse(Seq.empty))

    def decodeReply(mb: MultiBulk) = MultiBulkConverter.GeoNeighborWithdist(mb)

    val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class GeoradiusbymemberWithdist[K, V, R](key: K, member: V, radius: Double, unit: String, order: Option[Order])(implicit keySeria: ByteStringSerializer[K], convert: ByteStringSerializer[V], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandMultiBulk[Seq[(R, Double)]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("GEORADIUSBYMEMBER",
    Seq(keySeria.serialize(key), convert.serialize(member), ByteString(radius.toString), ByteString(unit), ByteString("WITHDIST")) ++ order.map(ord=>Seq(ByteString(ord.toString))).getOrElse(Seq.empty))

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.GeoNeighborWithdist(mb)

  val deserializer: ByteStringDeserializer[R] = deserializerR
}
