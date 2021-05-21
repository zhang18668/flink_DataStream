package hdpf.bean.source

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

case class Device(
                   @BeanProperty var device_name: String,
                   @BeanProperty var object_num: Int,
                   @BeanProperty var `object`: Array[Participant]
                 )

object Device {
  def apply(json: String): Device = {
    JSON.parseObject[Device](json, classOf[Device])
  }
}

