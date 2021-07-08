package hdpf.operator.map

import hdpf.bean.source.{LaneCar, Participant}
import org.apache.flink.api.common.functions.MapFunction
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}


class LaneCarFunction() extends MapFunction[Participant, LaneCar] {
  override def map(par: Participant): LaneCar = {
    val time = par.timestamp.toLong
    val id = par.id
    val longitude = par.location.longitude*10000000
    val latitude = par.location.latitude*10000000
    val speed = par.speed
    val hdq = par.arctan.hdq
    val pitch = par.arctan.pitch
    val Gx = 0
    val Gy = 0
    val yaw = 0
    val lane_id = 0
    val road_id = 0
    var delta_speed=0
    new LaneCar(time, id, longitude, latitude, speed, hdq, pitch, Gx, Gy, yaw, lane_id, road_id,delta_speed)
  }
}
