package hdpf.utils

import com.typesafe.config.{Config, ConfigFactory}

// 配置文件加载类
object GlobalConfigUtil {

  // 通过工厂加载配置
  val config:Config = ConfigFactory.load()

  val bootstrapServers = config.getString("bootstrap.servers")
  val zookeeperConnect = config.getString("zookeeper.connect")
  val inputTopic = config.getString("input.topic")
  val groupId = config.getString("group.id")
  val enableAutoCommit = config.getString("enable.auto.commit")
  val autoCommitIntervalMs = config.getString("auto.commit.interval.ms")
  val autoOffsetReset = config.getString("auto.offset.reset")
 //mysql 配置
  val mysql_driver: String = config.getString("mysql_driver")
  val msyql_url: String = config.getString("msyql_url")
  val msyql_user: String = config.getString("msyql_user")
  val msyql_password: String = config.getString("msyql_passwd")
  val queuelength=config.getString("queuelength")
  val stopnumber=config.getString("stopnumber")
  val trafficvolume=config.getString("trafficvolume")
  val stopdelay=config.getString("stopdelay")
  //时间窗口配置

  val windowDuration: Int = config.getInt("window.duration")
  val windowTimeStep: Int = config.getInt("window.time.step")
  val jobName: String = config.getString("flink.job.name")

  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
  }

}
