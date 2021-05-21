package hdpf.bean.source

import com.alibaba.fastjson.JSON


case class Message(
                    var vhost: String,
                    var name: String,
                    var payload: String
                  )

object Message {
  def apply(json: String): Message = {
    JSON.parseObject[Message](json, classOf[Message])
  }
}