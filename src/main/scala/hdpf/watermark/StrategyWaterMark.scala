package hdpf.watermark

import hdpf.bean.{Message, Payload}
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner


class StrategyWaterMark extends SerializableTimestampAssigner[Message] {

  override def extractTimestamp(element: Message, recordTimestamp: Long): Long = {
    Payload(element.payload).time.toLong
  }
}
