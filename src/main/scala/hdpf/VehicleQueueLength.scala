package hdpf

import hdpf.bean.{Device, Participant, Payload, Statistics}
import hdpf.operator.{AllWindowApply, IsInLane01}
import hdpf.sink.MySqlSink
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import hdpf.watermark.{AssginerWaterMark, AssginerWaterMarkVersion2}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory


object VehicleQueueLength {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    //TODO 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    kafkaDataStream.print("kafkaDataStream")

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
    //    canalDs.print("canalDs")
    val messagesDS: DataStream[Payload] = canalDs.filter(mes => if (mes.version == null || mes.time == null) false else true)
    //    waterDs.print("waterDs")
    val deviceDS: DataStream[(Device, String)] = messagesDS.flatMap(x => x.device_data.map(y => (y, x.time)))
    //    deviceDS.print("deviceDS")
    val participantDS: DataStream[(Participant, String)] = deviceDS.flatMap(x => x._1.`object`.map(y => (y, x._2)))
//    val parDS: DataStream[Participant] = participantDS.map(x => new Participant(x._1.`type`, x._1.license_plate, x._1.id, x._1.pose, x._1.location, x._1.arctan, x._1.conf, x._1.speed, x._2))
    //    parDS.print("parDS")
//    val parWaterDS = parDS.assignTimestampsAndWatermarks(new AssginerWaterMark)
//    val parSpeedDS = parDS.filter(_.speed == 0)
//    val arrFilter: DataStream[Participant] = parSpeedDS.filter(new IsInLane01)
//    arrFilter.print("arr")

//    winDS.addSink(new MySqlSink)
    //    winDS.addSink()

    // 执行任务
    env.execute(GlobalConfigUtil.jobName)
  }
}
