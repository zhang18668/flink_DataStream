package hdpf.bean.source

import com.alibaba.fastjson.JSON

case class Pose(
                 var length: Double,
                 var width: Double,
                 var height: Double
               )

object Pose {
  def apply(json: String): Pose = {
    JSON.parseObject[Pose](json, classOf[Pose])
  }
}