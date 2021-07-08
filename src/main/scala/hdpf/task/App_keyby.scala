package hdpf.task

import java.util.concurrent.TimeUnit

import hdpf.bean.enity.Point
import hdpf.bean.sink.{QueueLength, TrafficVolume}
import hdpf.bean.source.{Device, LaneCar, Participant, Payload}
import hdpf.operator.fitter.IsInPloyin
import hdpf.operator.map.{JOSNFunction, JOSNStringFunction, LaneCarFunction, QueueLengthFunction}
import hdpf.operator.window.allWindow.{StopDelayAllWindowApply, StopNumAllWindowApply, TrafficVolumeAllWindowApply}
import hdpf.operator.window.window.{SpeedWindowFunction, StopDelayWindowFunction}
import hdpf.sink.{MySqlQueueLengthSink, StopDelayMysqlSink, StopNumMysqlSink, TrafficVolumeMySqlSink}
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


object App_keyby {
  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO 1. Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // TODO 2. 整合MQ
    val connectionConfig = FlinkUtils.initMQFlink()

    val stream = env.addSource(new RMQSource[String](connectionConfig, GlobalConfigUtil.rabbitmqQueueName, false, new SimpleStringSchema)).setParallelism(1)

//    stream.print("原始")
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


//    val fileDS: DataStream[String] = parDS.map(_.par_string())
//    val fileJsonDS: DataStream[String] = parDS.map(new JOSNStringFunction)
//    fileJsonDS.print("fileJsonDS")
//    fileDS.addSink(FlinkUtils.initFileSink())
//    fileDS.addSink(FlinkUtils.producerKafkaFlink(GlobalConfigUtil.outputTopic))
//    fileJsonDS.addSink(FlinkUtils.producerKafkaFlink(GlobalConfigUtil.jsonoutputTopic))


    val laneCarDS: DataStream[LaneCar] = parDS.map(new LaneCarFunction)
    val laneCarSpeedDS: DataStream[ListBuffer[LaneCar]] = laneCarDS.keyBy(_.lane_id).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new SpeedWindowFunction())

    val laneCarSpeedJsonDS: DataStream[String] = laneCarSpeedDS.flatMap(x=>x).map(new JOSNFunction())
    laneCarSpeedJsonDS.print("laneCarSpeedJsonDS")
    laneCarSpeedJsonDS.addSink(FlinkUtils.producerKafkaFlink(GlobalConfigUtil.jsonoutputTopic))
/***
    //根据车道号计算停车延迟
    val laneidStopDelayDS = laneCarDS.keyBy(_.lane_id).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new StopDelayWindowFunction())
    laneidStopDelayDS.print("laneidStopDelayDS")
    laneidStopDelayDS.addSink(new StopDelayMysqlSink)

    //    根据道路计算停车延迟

    val roadidStopDelayDS = laneCarDS.keyBy(_.road_id).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new StopDelayWindowFunction())
    roadidStopDelayDS.print("laneidStopDelayDS")
    roadidStopDelayDS.addSink(new StopDelayMysqlSink)

    //    根据道路
***/
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
