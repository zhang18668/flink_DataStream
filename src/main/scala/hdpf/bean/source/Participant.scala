package hdpf.bean.source

import com.alibaba.fastjson.JSON

case class Participant(
                        var `type`: String,
                        var license_plate: String,
                        var id: String,
                        var pose: Pose,
                        var location: Location,
                        var arctan: Arctan,
                        var conf: Double,
                        var speed: Double,
                        var timestamp: String
                      )

object Participant {
  def apply(json: String): Participant = {
    JSON.parseObject[Participant](json, classOf[Participant])
  }
}