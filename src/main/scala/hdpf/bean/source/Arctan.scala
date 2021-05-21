package hdpf.bean.source

import com.alibaba.fastjson.JSON

case class Arctan(
                   var vehicle_judge: String,
                   var hdq: Double,
                   var pitch: Double,
                   var roll: Double
                 )

object Arctan {
  def apply(json: String): Arctan = {
    JSON.parseObject[Arctan](json, classOf[Arctan])
  }
}