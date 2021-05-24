package hdpf.utils

import java.io.{IOException, InputStream}
import java.util.Properties

import org.apache.commons.lang3.SystemUtils


object ConfigLoader {
  /**
    * 开发步骤：
    * 1：使用classLoader加载类对象，然后加载conf.properties
    * 2：使用Properties的load方法加载inputStream
    * 3：编写方法获取配置项的key对应的value值
    * 4：编写方法获取int类型的key对应的value值
    */
  var propertiesName:String=_

  /**
    * static 静态模块
    * 自动判断本地环境还是生产环境
    */
  {
    if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
      //本地环境配置文件操作的对象
      propertiesName = "dev.properties"
    } else {
      //生产环境
      propertiesName = "conf.properties"
    }
  }
  //使用classLoader加载类对象，然后加载conf.properties
  val inputStream: InputStream = ConfigLoader.getClass.getClassLoader.getResourceAsStream("config.properties")

  //使用Properties的load方法加载inputStream
  val props = new Properties()

  try {
    //加载inputStream-》conf.properties
    props.load(inputStream)
  } catch {
    case ex: Exception => {
      ex.printStackTrace() // 打印到标准err
      System.err.println("exception===>: ...")  // 打印到标准err
    }
  }

  //编写方法获取配置项的key对应的value值
  def getString(key: String): String = props.getProperty(key)

  //编写方法获取int类型的key对应的value值
  def getInt(key: String): Int = props.getProperty(key).toInt

}
