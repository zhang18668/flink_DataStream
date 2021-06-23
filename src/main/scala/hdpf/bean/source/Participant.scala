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
                      ) {
  def par_string(): String = {
    //    var str=""
    //    逻辑
    println(this.arctan.pitch)
    val str = this.timestamp + "," + this.id + "," + this.location.longitude.toString + "," + this.location.latitude.toString + "," + this.speed.toString ++ "," + this.arctan.hdq.toString+ "," + this.arctan.pitch.toString+ "," + this.arctan.roll.toString+ ", , , , , "
    str
  }

  def json_string(): String = {
    //    var str=""
    //    逻辑
    val str = "{\"time\":" + this.timestamp+",\"id\":" + this.id+",\"lon\"" + this.location.longitude.toString+",\"lat\":" + this.location.latitude.toString+",\"speed\":" + this.speed.toString+",\"Gx\":" + " " + ",\"Gy\":" + " " + ",\"yaw\":" + " " + ",\"lane_id\":" + " "+"}"
    str
  }

}

object Participant {


  def apply(json: String): Participant = {
    JSON.parseObject[Participant](json, classOf[Participant])
  }


}