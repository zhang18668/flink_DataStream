package hdpf.watermark

import hdpf.bean.{Message, Payload}
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner


class StrategyWaterMark extends SerializableTimestampAssigner[Payload] {

  override def extractTimestamp(element: Payload, recordTimestamp: Long): Long = {
    element.time.toLong
  }
}
