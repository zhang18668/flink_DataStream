package hdpf.utils

import java.util.Properties
import java.util.concurrent.TimeUnit

import hdpf.utils.ConfigLoader.propertiesName
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object FlinkUtils {

  // 初始化flink的流式环境
  def initFlinkEnv() = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
    * 本地环境和生产环境自动识别
     */
    {
      if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
        //本地环境配置文件操作的对象
        //        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置env的处理时间为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      } else {
        //生产环境
        //        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置env的处理时间为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //  设置并行度
        env.setParallelism(3)
        //
        // 设置checkpoint
        // 开启checkpoint,间隔时间为5s
        env.enableCheckpointing(5000)
        // 设置处理模式
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        // 设置两次checkpoint的间隔
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
        // 设置超时时长
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        // 设置并行度
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        // 当程序关闭的时候,触发额外的checkpoint
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        // 设置检查点在hdfs中存储的位置
        env.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/flink-checkpoint"))

      }
    }


    env
  }


  def initKafkaFlink() = {
    // 整合Kafka
    val props: Properties = new Properties()


    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    props.setProperty("group.id", GlobalConfigUtil.groupId)
    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)


    // String topic, DeserializationSchema<T> valueDeserializer, Properties props
    val consumer = new FlinkKafkaConsumer[String](
      GlobalConfigUtil.inputTopic,
      new SimpleStringSchema(),
      props
    )
    //    consumer.setStartFromEarliest()
    consumer.setStartFromLatest()
    consumer
  }
  def consumerKafkaFlink(topic: String) = {
    // 整合Kafka
    val props: Properties = new Properties()


    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    props.setProperty("group.id", GlobalConfigUtil.groupId)
    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)


    // String topic, DeserializationSchema<T> valueDeserializer, Properties props
    val consumer = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      props
    )
    //    consumer.setStartFromEarliest()
    consumer.setStartFromLatest()
    consumer
  }
  def producerKafkaFlink(topic: String) = {
    // 整合Kafka
    val props: Properties = new Properties()


    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
//    props.setProperty("group.id", GlobalConfigUtil.groupId)
//    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
//    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("transaction.timeout.ms", GlobalConfigUtil.autoCommitIntervalMs)
//    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)

    // String topic, DeserializationSchema<T> valueDeserializer, Properties props
    val producer  = new FlinkKafkaProducer[String](
      topic,
      new SimpleStringSchema(),
      props
    )
    producer

  }
  def initMQFlink(): RMQConnectionConfig = {
    // 整合mq
    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost(GlobalConfigUtil.rabbitmqHost)
      .setPort(GlobalConfigUtil.rabbitmqPort)
      .setVirtualHost(GlobalConfigUtil.rabbitmqVirtualHost)
      .setUserName(GlobalConfigUtil.rabbitmqUserName)
      .setPassword(GlobalConfigUtil.rabbitmqPassword)
      .build
    connectionConfig
  }

  def initFileSink(): StreamingFileSink[String] = {
    // 整合mq
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("hdfs://cdh01:8020/hdpf/ods/table/date"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(1)) //10min 生成一个文件
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) //5min未接收到数据，生成一个文件
          .withMaxPartSize(1024 * 1024 * 1024) //文件大小达到1G
          .build())
      .build()
    sink
  }

}
