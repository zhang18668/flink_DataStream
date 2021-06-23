package hdpf.utils
import org.apache.http.client.HttpClient
import scalaj.http._
import scala.collection.immutable.StringLike
import java.io.IOException
import java.util
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{DefaultHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.reflect.macros.ParseException

object Http_flask {

  val logger = LoggerFactory.getLogger("out")

  def get(url: String): String = {
    val httpclient = new DefaultHttpClient()
    try {
      // 创建httpget.
      val httpget = new HttpGet(url)
      // 执行get请求.
      val response = httpclient.execute(httpget)
      try {
        // 获取响应实体
        val entity = response.getEntity()
        EntityUtils.toString(entity, "utf-8")
      } finally {
        response.close()
      }
    } catch {
      case ex: ClientProtocolException => {logger.error(ex.getMessage);null}
      case ex: ParseException => {logger.error(ex.getMessage);null}
      case ex: IOException => {logger.error(ex.getMessage);null}
    } finally {
      // 关闭连接,释放资源
      httpclient.close()
    }

  }

  def post(url: String, map: Map[String,String]): String = {
    //创建httpclient对象
    val client = HttpClients.createDefault
    try {
      //创建post方式请求对象
      val httpPost = new HttpPost(url)
      //装填参数
      val nvps:util.ArrayList[BasicNameValuePair] = new util.ArrayList[BasicNameValuePair]
      if (map != null) {
        for (entry <- map.entrySet) {
          println(entry)
          nvps.add(new BasicNameValuePair(entry.getKey, entry.getValue))
          println(entry.getKey, entry.getValue)
        }
      }
      //设置参数到请求对象中
      httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"))
      //执行请求操作，并拿到结果（同步阻塞）
      val response = client.execute(httpPost)
      //获取结果实体
      val entity = response.getEntity
      var body = ""
      if (entity != null) { //按指定编码转换结果实体为String类型
        body = EntityUtils.toString(entity, "UTF-8")
      }
      //释放链接
      response.close()
      body
    } finally {
      client.close()
    }
  }

  def main(args: Array[String]): Unit = {
//    val url="http://127.0.0.1:5000/test?name=1257067.3212221987&age=3535810.5023696893"
    val url="http://127.0.0.1:5000/test?name=%1$s&age=%2$s".format("1257067.3212221987","3535810.5023696893")
    val str1: String = get(url)
    val stringToString = Map("name" -> "1257067.3212221987", "age" -> "3535810.5023696893")
    val value = "http://127.0.0.1:5000/test"
    println(str1)
//    val str = post(value, stringToString)
//    println(str)
  }
}