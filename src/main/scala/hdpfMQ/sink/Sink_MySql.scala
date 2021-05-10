package hdpfMQ.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import hdpf.bean.Statistics
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
    val listDataSet: DataStream[Statistics] = env.fromCollection(List(
      Statistics("dazhuang", "123456", 48),
      Statistics("erya", "123456", 50),
      Statistics("sanpang", "123456", 60)
    ))

    listDataSet.print()
    // 3. add sink
    listDataSet.addSink(new MySqlSink)
    // 4. execute
    env.execute()

  }

}

class MySqlSink extends RichSinkFunction[Statistics] {

  var connection: Connection = null;
  var ps: PreparedStatement = null;

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName(GlobalConfigUtil.mysql_driver)
    // 2. 创建连接
    var connection = DriverManager.getConnection(GlobalConfigUtil.msyql_url, GlobalConfigUtil.msyql_user, GlobalConfigUtil.msyql_password)
    // 3. 创建PreparedStatement
    val tablename=GlobalConfigUtil.tablename
    val sql = "insert into "+tablename+"(startTime,endTime,result) values(?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: Statistics): Unit = {
    println("SQL执行")
    // 执行插入
    ps.setString(1, value.startTime)
    ps.setString(2, value.endTime)
    ps.setInt(3, value.result)

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
