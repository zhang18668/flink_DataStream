package hdpf.bean.source

import com.alibaba.fastjson.JSON


case class Payload(
                    var version: String,
                    var time: String,
                    var device_data: Array[Device]
                  )

object Payload {
  def apply(json: String): Payload = {
    JSON.parseObject[Payload](json, classOf[Payload])
  }
}