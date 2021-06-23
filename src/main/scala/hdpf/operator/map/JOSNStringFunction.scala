package hdpf.operator.map


import hdpf.bean.source.{LaneCar, Participant}
import hdpf.utils.Http_flask
import org.apache.flink.api.common.functions.MapFunction
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write


class JOSNStringFunction() extends MapFunction[Participant, String] {
  override def map(par: Participant): String = {
    val time = par.timestamp.toLong
    val id = par.id
    val longitude = par.location.longitude
    val latitude = par.location.latitude
    val speed = par.speed
    val hdq = par.arctan.hdq
    val pitch = par.arctan.pitch
    val Gx = 0
    val Gy = 0
    val yaw = 0
    val url = "http://127.0.0.1:5000/lane?longitude=%1$s&latitude=%2$s".format(longitude.toString, latitude.toString)
//    val lanestr: String = Http_flask.get(url)
//    val lanesplit = lanestr.split(",")
//    val lane_id = lanesplit(0).toInt
//    val road_id = lanesplit(1).toInt
    val lane_id = 0
    val road_id = 0
    val car = new LaneCar(time, id, longitude, latitude, speed, hdq, pitch, Gx, Gy, yaw, lane_id, road_id)
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    write(car)
  }
}
