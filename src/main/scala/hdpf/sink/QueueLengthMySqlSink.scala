package hdpf.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import hdpf.bean.sink.{QueueLength, TrafficVolume}
import hdpf.utils.GlobalConfigUtil
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object MySqlQueueLength {

  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. load list
    val listDataSet = env.fromCollection(List(
      QueueLength("1620703103515", 48.1D,1),
      QueueLength("1620703103316", 50D,2),
      QueueLength("1620703104211", 60D,3)
    ))

    listDataSet.print()
    // 3. add sink
    listDataSet.addSink(new MySqlQueueLengthSink)
    // 4. execute
    env.execute()

  }

}

class MySqlQueueLengthSink extends RichSinkFunction[QueueLength] {

  var connection: Connection = _
  var ps: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName(GlobalConfigUtil.mysql_driver)
    // 2. 创建连接
    var connection = DriverManager.getConnection(GlobalConfigUtil.msyql_url, GlobalConfigUtil.msyql_user, GlobalConfigUtil.msyql_password)
    // 3. 创建PreparedStatement
    val tablename=GlobalConfigUtil.queuelength
    val sql = "insert into "+tablename+"(timestamp,queueLength,roadId) values(?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: QueueLength): Unit = {
    println("SQL执行")
    // 执行插入
    ps.setString(1, value.timestamp)
    ps.setDouble(2, value.queueLength)
    ps.setInt(3, value.roadId)


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
