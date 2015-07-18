package redis.commands

import akka.util.ByteString
import redis.RedisSpec
import redis.api.ASC

import scala.concurrent.Await

class GeoSpec extends RedisSpec {
  "Geo commands" should {
    "GEOADD" in {
      val r = for {
        _ <- redis.del("geoaddKey")
        g0 <- redis.geoadd("geoaddKey", (13.361389, 38.115556, "Palermo"))
        g1 <- redis.geoadd("geoaddKey", (13.3613, 38.1155,"Palermo modified"), (15.087269,37.502669,"Catania"))
//        zr <- redis.zrangeWithscores("zaddKey", 0, -1)
      } yield {
          g0 mustEqual 1
          g1 mustEqual 2
//          zr mustEqual Seq((ByteString("one"), 1), (ByteString("uno"), 1), (ByteString("two"), 3))
        }
      Await.result(r, timeOut)
    }

    "GEODIST" in {
      val r = for {
        _ <- redis.del("geodistKey")
        _ <- redis.geoadd("geodistKey", (13.361389, 38.115556, "Palermo"), (15.087269,37.502669,"Catania"))
        g1 <- redis.geodist("geodistKey", "Palermo", "Catania")
        g2 <- redis.geodist("geodistKey", "Palermo", "Catania", "km")
        g3 <- redis.geodist("geodistKey", "Palermo", "Catania", "mi")
      //        zr <- redis.zrangeWithscores("zaddKey", 0, -1)
      } yield {
//          g0 mustEqual 0
          g1 mustEqual Some(166274.15156960033)
          g2 mustEqual Some(166.27415156960032)
          g3 mustEqual Some(103.31822459492733)
          //          zr mustEqual Seq((ByteString("one"), 1), (ByteString("uno"), 1), (ByteString("two"), 3))
        }
      Await.result(r, timeOut)
    }

    "GEOHASH" in {
      val r = for {
        _ <- redis.del("geohashKey")
        _ <- redis.geoadd("geohashKey", (13.361389, 38.115556, "Palermo"), (15.087269,37.502669,"Catania"))
        g1 <- redis.geohash("geohashKey", "Palermo", "Catania")
        g2 <- redis.geohash("geohashKey", "Palermo")
        g3 <- redis.geohash("geohashKey", "Palermo", "unknown")
      } yield {
          //          g0 mustEqual 0
          g1 mustEqual Seq(Some("sqc8b49rny0"), Some("sqdtr74hyu0"))
          g2 mustEqual Seq(Some("sqc8b49rny0"))
          g3 mustEqual Seq(Some("sqc8b49rny0"), None)
          //          zr mustEqual Seq((ByteString("one"), 1), (ByteString("uno"), 1), (ByteString("two"), 3))
        }
      Await.result(r, timeOut)
    }

    "GEOPOS" in {
      val r = for {
        _ <- redis.del("geoposKey")
        _ <- redis.geoadd("geoposKey", (13.361389, 38.115556, "Palermo"), (15.087269,37.502669,"Catania"))
        g1 <- redis.geopos("geoposKey", "Palermo", "Catania")
        g2 <- redis.geopos("geoposKey", "Palermo")
        g3 <- redis.geopos("geoposKey", "Palermo", "unknown")
        g4 <- redis.geopos("geoposKey", "unknown")
      } yield {
          g1 mustEqual Seq(Some((13.361389338970184,38.1155563954963)), Some((15.087267458438873,37.50266842333162)))
          g2 mustEqual Seq(Some((13.361389338970184,38.1155563954963)))
          g3 mustEqual Seq(Some((13.361389338970184,38.1155563954963)), None)
          g4 mustEqual Seq(None)
        }
      Await.result(r, timeOut)
    }

    "GEORADIUS WITHDIST" in {
      val r = for {
        _ <- redis.del("georadiuswithdistKey")
        _ <- redis.geoadd("georadiuswithdistKey", (13.361389, 38.115556, "Palermo"), (15.087269,37.502669,"Catania"))
        g1 <- redis.georadiusWithdist("georadiuswithdistKey", 15, 37, 200, "km")
        g2 <- redis.georadiusWithdist("georadiuswithdistKey", 15, 37, 200, "km", Some(ASC))
      } yield {
          g1 mustEqual Seq((ByteString("Palermo"), 190.4424), (ByteString("Catania"),56.4413))
          g2 mustEqual Seq((ByteString("Catania"),56.4413), (ByteString("Palermo"), 190.4424))
        }
      Await.result(r, timeOut)
    }

    "GEORADIUSBYMEMBER WITHDIST" in {
      val r = for {
        _ <- redis.del("georadiusbymemberwithdistKey")
        _ <- redis.geoadd("georadiusbymemberwithdistKey", (13.361389, 38.115556, "Palermo"), (15.087269,37.502669,"Catania"))
        g1 <- redis.georadiusbymemberWithdist("georadiusbymemberwithdistKey", "Palermo", 200, "km")
        g2 <- redis.georadiusbymemberWithdist("georadiusbymemberwithdistKey", "Unknown", 200, "km")
      } yield {
          g1 mustEqual Seq((ByteString("Palermo"), 190.4424), (ByteString("Catania"),56.4413))
          g2 mustEqual Seq()
        }
      Await.result(r, timeOut)
    }


  }
}
