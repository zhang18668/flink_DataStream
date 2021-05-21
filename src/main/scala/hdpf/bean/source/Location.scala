package hdpf.bean.source

import com.alibaba.fastjson.JSON

case class Location(
                     var vehicle_judge: String,
                     var longitude: Double,
                     var latitude: Double,
                     var altitude: Double,
                     var headroom: Int
                   )

object Location {
  def apply(json: String): Location = {
    JSON.parseObject[Location](json, classOf[Location])
  }
}

