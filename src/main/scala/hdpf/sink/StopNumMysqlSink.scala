package hdpf.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import hdpf.bean.sink.{QueueLength, StopNumber}
import hdpf.utils.GlobalConfigUtil
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StopNumMysqlSinkDemo {

  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. load list
    val listDataSet = env.fromCollection(List(
      StopNumber("1620703103515","1620703103515", 48, 1),
      StopNumber("1620703103316","1620703103515", 50, 2),
      StopNumber("1620703104211","1620703103515", 60, 3)
    ))

    listDataSet.print()
    // 3. add sink
    listDataSet.addSink(new StopNumMysqlSink)
    // 4. execute
    env.execute()

  }

}

class StopNumMysqlSink extends RichSinkFunction[StopNumber] {

  var connection: Connection = null;
  var ps: PreparedStatement = null;

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName(GlobalConfigUtil.mysql_driver)
    // 2. 创建连接
    var connection = DriverManager.getConnection(GlobalConfigUtil.msyql_url, GlobalConfigUtil.msyql_user, GlobalConfigUtil.msyql_password)
    // 3. 创建PreparedStatement
    val tablename = GlobalConfigUtil.stopnumber
    val sql = "insert into " + tablename + "(startTime,endTime,averStopNum,classfiy) values(?,?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: StopNumber): Unit = {
    println("SQL执行")
    // 执行插入
    ps.setString(1, value.startTime)
    ps.setString(2, value.endTime)
    ps.setInt(3, value.averStopNum)
    ps.setInt(4, value.classfiy)


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
