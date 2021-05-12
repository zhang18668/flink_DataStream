package hdpf.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import hdpf.bean.sink.TrafficVolume
import hdpf.utils.GlobalConfigUtil
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Sink_MySql {

  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. load list
    val listDataSet: DataStream[TrafficVolume] = env.fromCollection(List(
      TrafficVolume("dazhuang", "123456", 48,1),
      TrafficVolume("erya", "123456", 50,1),
      TrafficVolume("sanpang", "123456", 60,1)
    ))

    listDataSet.print()
    // 3. add sink
    listDataSet.addSink(new TrafficVolumeMySqlSink)
    // 4. execute
    env.execute()

  }

}

class TrafficVolumeMySqlSink extends RichSinkFunction[TrafficVolume] {

  var connection: Connection = _
  var ps: PreparedStatement =_

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName(GlobalConfigUtil.mysql_driver)
    // 2. 创建连接
    var connection = DriverManager.getConnection(GlobalConfigUtil.msyql_url, GlobalConfigUtil.msyql_user, GlobalConfigUtil.msyql_password)
    // 3. 创建PreparedStatement
    val tablename=GlobalConfigUtil.trafficflow
    val sql = "insert into "+tablename+"(startTime,endTime,result,roadId) values(?,?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: TrafficVolume): Unit = {
    println("SQL执行")
    // 执行插入
    ps.setString(1, value.startTime)
    ps.setString(2, value.endTime)
    ps.setInt(3, value.result)
    ps.setInt(4, value.roadId)

    ps.executeUpdate()
  }


  override def close(): Unit = {
    // 关闭连接
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }


}
