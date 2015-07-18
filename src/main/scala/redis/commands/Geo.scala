package redis.commands

import redis.api.Order
import redis.{ByteStringDeserializer, ByteStringSerializer, Request}
import scala.concurrent.Future
import redis.api.geo._

trait Geo extends Request {

  def geoadd[V: ByteStringSerializer](key: String, geoMembers: (Double, Double, V)*): Future[Long] =
    send(Geoadd(key, geoMembers))

  def geodist[V: ByteStringSerializer](key: String, member1: V, member2: V, unit: String = "m"): Future[Option[Double]] =
    send(Geodist(key, member1, member2, unit))

  def geohash[V: ByteStringSerializer](key: String, members: V*): Future[Seq[Option[String]]] =
    send(Geohash(key, members))

  def geopos[V: ByteStringSerializer](key: String, members: V*): Future[Seq[Option[(Double, Double)]]] =
    send(Geopos(key, members))

  def georadiusWithdist[R: ByteStringDeserializer](key: String, longitude: Double, latitude: Double, radius: Double, unit: String = "m", order: Option[Order] = None): Future[Seq[(R, Double)]] =
    send(GeoradiusWithdist(key, longitude, latitude, radius, unit, order))

  def georadiusbymemberWithdist[V: ByteStringSerializer, R: ByteStringDeserializer](key: String, member: V, radius: Double, unit: String = "m", order: Option[Order] = None): Future[Seq[(R, Double)]] =
    send(GeoradiusbymemberWithdist(key, member, radius, unit, order))
}

