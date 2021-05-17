package hdpf.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import hdpf.bean.sink.StopDelay
import hdpf.utils.GlobalConfigUtil
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StopDelayMysqlSinkDemo {

  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. load list
    val listDataSet = env.fromCollection(List(
      StopDelay("1620703103515","1620703103515", 48D,2D ,1D,1),
      StopDelay("1620703103316","1620703103515", 50D,3D,2D,2),
      StopDelay("1620703104211","1620703103515", 60D, 4D,2D,3)
    ))

    listDataSet.print()
    // 3. add sink
    listDataSet.addSink(new StopDelayMysqlSink)
    // 4. execute
    env.execute()

  }

}

class StopDelayMysqlSink extends RichSinkFunction[StopDelay] {

  var connection: Connection = _
  var ps: PreparedStatement =_

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName(GlobalConfigUtil.mysql_driver)
    // 2. 创建连接
    var connection = DriverManager.getConnection(GlobalConfigUtil.msyql_url, GlobalConfigUtil.msyql_user, GlobalConfigUtil.msyql_password)
    // 3. 创建PreparedStatement
    val tablename = GlobalConfigUtil.stopdelay
    val sql = "insert into " + tablename + "(startTime,endTime,maxDelay,minDelay,avgDelay,roadId) values(?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: StopDelay): Unit = {
    println("SQL执行")
    // 执行插入
    ps.setString(1, value.startTime)
    ps.setString(2, value.endTime)
    ps.setDouble(3, value.maxDelay)
    ps.setDouble(4, value.minDelay)
    ps.setDouble(5, value.avgDelay)
    ps.setInt(6, value.roadId)


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
