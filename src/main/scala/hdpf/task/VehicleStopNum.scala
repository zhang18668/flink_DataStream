package hdpf.task

import hdpf.bean.sink.QueueLength
import hdpf.bean.source.{Device, Payload}
import hdpf.operator.map.QueueLengthFunction
import hdpf.sink.MySqlQueueLengthSink
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.slf4j.LoggerFactory


object VehicleStopNum {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    //TODO 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
//    kafkaDataStream.print("kafkaDataStream")

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
    val messagesDS: DataStream[Payload] = canalDs.filter(mes => if (mes.version == null || mes.time == null||mes.device_data==null) false else true)
    //    waterDs.print("waterDs")
    val devTupleDS: DataStream[(Device, String)] = messagesDS.flatMap(x => x.device_data.map(y => (y, x.time)))
//    返回距离与时间戳DS
    val distanceDS: DataStream[QueueLength] = devTupleDS.map(new QueueLengthFunction)
//    distanceDS.print("distance")
    val queueLenDS = distanceDS.filter(_.queueLength!=0D)
    queueLenDS.print("queueLenDS")
    queueLenDS.addSink(new MySqlQueueLengthSink)
//最后将数据添加到mysql sink
    //TODO
    // 执行任务
    env.execute(GlobalConfigUtil.jobName)
  }
}
