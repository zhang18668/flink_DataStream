package hdpf.task

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.serializer.SerializerFeature
import hdpf.bean.enity.Point
import hdpf.bean.sink.{QueueLength, TrafficVolume}
import hdpf.bean.source.{Device, Participant, Payload}
import hdpf.constant.Constant
import hdpf.operator.fitter.IsInPloyin
import hdpf.operator.map.{JOSNStringFunction, QueueLengthFunction}
import hdpf.operator.window.allWindow.{StopDelayAllWindowApply, StopNumAllWindowApply, TrafficVolumeAllWindowApply}
import hdpf.sink.{MySqlQueueLengthSink, StopDelayMysqlSink, StopNumMysqlSink, TrafficVolumeMySqlSink}
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import hdpf.watermark.ParticipantAssginerWaterMark
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.LoggerFactory
import com.alibaba.fastjson.{JSON, JSONObject}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write


object App {
  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO 1. Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // TODO 2. 整合MQ
    val connectionConfig = FlinkUtils.initMQFlink()

    val stream = env.addSource(new RMQSource[String](connectionConfig, GlobalConfigUtil.rabbitmqQueueName, false, new SimpleStringSchema)).setParallelism(1)

    stream.print("原始")
    //TODO 以下后期会转化
    //将原始数据转化为 Payload  对象 并过滤脏数据
    val canalDs = stream.map {
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


    //TODO 以上都是需要重构  以下都是一样

    //    加一个逻辑，写入到hdpf/ods/table/date


    val fileDS: DataStream[String] = parDS.map(_.par_string())
    val fileJsonDS: DataStream[String] = parDS.map(new JOSNStringFunction)
    fileJsonDS.print("fileJsonDS")
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("hdfs://cdh01:8020/hdpf/ods/table/date"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(1)) //10min 生成一个文件
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) //5min未接收到数据，生成一个文件
          .withMaxPartSize(1024 * 1024 * 1024) //文件大小达到1G
          .build())
      .build()
    fileDS.print("haha")
    fileDS.addSink(sink)
    fileDS.addSink(FlinkUtils.producerKafkaFlink(GlobalConfigUtil.outputTopic))
    fileJsonDS.addSink(FlinkUtils.producerKafkaFlink(GlobalConfigUtil.jsonoutputTopic))



    // 执行任务
    env.execute(GlobalConfigUtil.jobName)

  }























  def processRoad(devTupleDS: DataStream[(Device, String)], parWaterDS: DataStream[Participant], value: (List[Point], Int, Int)): Unit = {


    //    排队长度指标计算
    val distanceDS: DataStream[QueueLength] = devTupleDS.map(new QueueLengthFunction(value))
    //    排队长度指标过滤
    val queueLenDS = distanceDS.filter(_.queueLength != 0D)
    queueLenDS.addSink(new MySqlQueueLengthSink)
    //    交通流指标数据过滤
    val ptsDS: DataStream[Participant] = parWaterDS.filter(new IsInPloyin(value._1))
    val valueDS: WindowedStream[Participant, String, TimeWindow] = ptsDS.keyBy(_.id).window(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep)))
    val value1DS: AllWindowedStream[Participant, TimeWindow] = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep)))
    //停车延迟指标计算
    val stopDelayDS = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new StopDelayAllWindowApply)
    stopDelayDS.addSink(new StopDelayMysqlSink)
    //停车次数指标计算
    val stopNumDS = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new StopNumAllWindowApply)
    stopNumDS.addSink(new StopNumMysqlSink)
    //    交通流指标计算
    val trafficVolumeDS: DataStream[TrafficVolume] = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new TrafficVolumeAllWindowApply)
    trafficVolumeDS.addSink(new TrafficVolumeMySqlSink)

  }

  def processRoadCross(parWaterDS: DataStream[Participant], value: (List[Point], Int, Int)): Unit = {
    //    交通流指标数据过滤
    val ptsDS: DataStream[Participant] = parWaterDS.filter(new IsInPloyin(value._1))
    //    交通流指标计算
    val trafficVolumeDS: DataStream[TrafficVolume] = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new TrafficVolumeAllWindowApply)
    trafficVolumeDS.addSink(new TrafficVolumeMySqlSink)

  }

  def processLane(devTupleDS: DataStream[(Device, String)], parWaterDS: DataStream[Participant], value: (List[Point], Int, Int)): Unit = {


    //    排队长度指标计算
    val distanceDS: DataStream[QueueLength] = devTupleDS.map(new QueueLengthFunction(value))
    //    排队长度指标过滤
    val queueLenDS = distanceDS.filter(_.queueLength != 0D)
    queueLenDS.addSink(new MySqlQueueLengthSink)
    //    交通流指标数据过滤
    val ptsDS: DataStream[Participant] = parWaterDS.filter(new IsInPloyin(value._1))
    //停车延迟指标计算
    val stopDelayDS = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new StopDelayAllWindowApply)
    stopDelayDS.addSink(new StopDelayMysqlSink)
    //停车次数指标计算
    val stopNumDS = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new StopNumAllWindowApply)
    stopNumDS.addSink(new StopNumMysqlSink)
    //    交通流指标计算
    val trafficVolumeDS: DataStream[TrafficVolume] = ptsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(GlobalConfigUtil.windowDuration), Time.seconds(GlobalConfigUtil.windowTimeStep))).apply(new TrafficVolumeAllWindowApply)
    trafficVolumeDS.addSink(new TrafficVolumeMySqlSink)

  }


  def fileSink(outputPath: String, parDS: DataStream[Participant]) {

    val fileDS = parDS.map(_.toString)
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //10min 生成一个文件
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //5min未接收到数据，生成一个文件
          .withMaxPartSize(1024 * 1024 * 1024) //文件大小达到1G
          .build())
      .build()

    fileDS.addSink(sink)

  }
}
