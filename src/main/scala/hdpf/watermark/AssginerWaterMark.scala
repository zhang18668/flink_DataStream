package hdpf.watermark

import hdpf.bean.{Message, Payload}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


class AssginerWaterMark extends AssignerWithPeriodicWatermarks[Message] {

  // 当前的时间戳
  var currentTimestamp = 0L

  // 延迟的时间
  val delayTime = 2000l

  // 返回水印时间
  override def getCurrentWatermark: Watermark = {
    new Watermark(currentTimestamp - delayTime)
  }

  // 比较当前元素的时间和上一个元素的时间,取最大值,防止时光倒流
  override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {
    var time = Payload(element.payload).time.toLong
    currentTimestamp = Math.max(time, previousElementTimestamp)
    currentTimestamp
  }
}
