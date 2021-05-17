package hdpf.task

import hdpf.bean.source.{Device, Participant, Payload}
import hdpf.operator.fitter.IsInLane01
import hdpf.operator.window.allWindow.{StopDelayAllWindowApply, StopNumAllWindowApply}
import hdpf.sink.{StopDelayMysqlSink, StopNumMysqlSink}
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import hdpf.watermark.ParticipantAssginerWaterMark
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory


object VehicleStopDelay {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    //TODO 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    //    kafkaDataStream.print("kafkaDataStream")
    //将原始数据转化为 Payload  对象 并过滤脏数据
    val canalDs = kafkaDataStream.map {
      json => {
        var mes = new Payload(null, null, null)
        try {
          mes = Payload(json)
        }
        catch {
          case e: Exception => hdpfLogger.error("解析错误被舍弃!")
        }
        mes
      }
    }
    val messagesDS: DataStream[Payload] = canalDs.filter(mes => if (mes.version == null || mes.time == null || mes.device_data == null) false else true)
    //   将时间 映射到 每个 Device 对象中
    val devTupleDS: DataStream[(Device, String)] = messagesDS.flatMap(x => x.device_data.map(y => (y, x.time)))
    val parTupleDS: DataStream[(Participant, String)] = devTupleDS.flatMap(x => x._1.`object`.map(y => (y, x._2)))
    val parDS: DataStream[Participant] = parTupleDS.map(x => new Participant(x._1.`type`, x._1.license_plate, x._1.id, x._1.pose, x._1.location, x._1.arctan, x._1.conf, x._1.speed, x._2))
    val parWaterDS: DataStream[Participant] = parDS.assignTimestampsAndWatermarks(new ParticipantAssginerWaterMark)
    val parFitterDS = parWaterDS.filter(new IsInLane01)
    val parSinkDS = parFitterDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new StopDelayAllWindowApply)
    parSinkDS.addSink(new StopDelayMysqlSink )
    // 执行任务
    env.execute(GlobalConfigUtil.jobName)
  }
}
