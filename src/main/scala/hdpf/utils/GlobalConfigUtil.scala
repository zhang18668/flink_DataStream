package hdpf.utils

/**
  * 读取配置文件的工具类
  */
object GlobalConfigUtil {
  //kafka配置
  lazy val bootstrapServers: String = ConfigLoader.getString("bootstrap.servers")
  lazy val zookeeperConnect: String = ConfigLoader.getString("zookeeper.connect")
  lazy val inputTopic: String = ConfigLoader.getString("input.topic")
  lazy val groupId: String = ConfigLoader.getString("group.id")
  lazy val enableAutoCommit: String = ConfigLoader.getString("enable.auto.commit")
  lazy val autoCommitIntervalMs: String = ConfigLoader.getString("auto.commit.interval.ms")
  lazy val autoOffsetReset: String = ConfigLoader.getString("auto.offset.reset")
  lazy val outputTopic: String = ConfigLoader.getString("output.topic")
//  lazy val autoOffsetReset: String = ConfigLoader.getString("auto.offset.reset")
  //mysql 配置
  lazy val mysql_driver: String = ConfigLoader.getString("mysql_driver")
  lazy val msyql_url: String = ConfigLoader.getString("msyql_url")
  lazy val msyql_user: String = ConfigLoader.getString("msyql_user")
  lazy val msyql_password: String = ConfigLoader.getString("msyql_passwd")
  lazy val queuelength: String = ConfigLoader.getString("queuelength")
  lazy val stopnumber: String = ConfigLoader.getString("stopnumber")
  lazy val trafficvolume: String = ConfigLoader.getString("trafficvolume")
  lazy val stopdelay: String = ConfigLoader.getString("stopdelay")
  //时间窗口配置
  lazy val windowDuration: Int = ConfigLoader.getInt("window.duration")
  lazy val windowTimeStep: Int = ConfigLoader.getInt("window.time.step")
  lazy val jobName: String = ConfigLoader.getString("flink.job.name")
  //rabbitMQ配置
  lazy val rabbitmqHost: String = ConfigLoader.getString("rabbitmq.host")
  lazy val rabbitmqPort: Int = ConfigLoader.getInt("rabbitmq.port")
  lazy val rabbitmqVirtualHost: String = ConfigLoader.getString("rabbitmq.virtual.host")
  lazy val rabbitmqUserName: String = ConfigLoader.getString("rabbitmq.user.name")
  lazy val rabbitmqPassword: String = ConfigLoader.getString(" rabbitmq.password")
  lazy val rabbitmqQueueName: String = ConfigLoader.getString(" rabbitmq.queue.name")


  def main(args: Array[String]): Unit = {
    println("DB_ORACLE_URL = " + bootstrapServers)
    println("KAFKA_ADDRESS = " + groupId)
  }
}
