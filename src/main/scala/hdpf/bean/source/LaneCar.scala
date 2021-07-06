package hdpf.bean.source

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import lombok.Data
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.beans.BeanProperty


@Data
@BeanProperty
case class LaneCar(
                    var time: Long,
                    var id: String,
                    var longitude: Double,
                    var latitude: Double,
                    var speed: Double,
                    var hdq: Double,
                    var pitch: Double,
                    var Gx: Int,
                    var Gy: Int,
                    var yaw: Int,
                    var lane_id: Long,
                    var road_id: Long,
                    var delta_speed: Double
                  ) {

}

object LaneCar {
  def main(args: Array[String]): Unit = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val car = new LaneCar(1, "2", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,11.1)
    car.time = 2
    println(car)
    val hbaseStr = write(car)
    val str = car.toString()
    println(hbaseStr)
    println(str)
  }
}