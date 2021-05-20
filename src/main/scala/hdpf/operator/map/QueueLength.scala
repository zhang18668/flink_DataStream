package hdpf.operator.map

import hdpf.bean.sink.QueueLength
import hdpf.bean.source.{Device, Participant}
import hdpf.utils.Meter
import hdpf.utils.Meter.Point
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable.ArrayBuffer

class QueueLengthFunction extends MapFunction[(Device, String),QueueLength] {
  override def map(t: (Device, String)): QueueLength ={
      //    将所有的汽车Participant 对象集合取出
      val parts: Array[Participant] = t._1.`object`
    //    路口1 对应的group.id="intersection01"  tablename="intersection01"
    //    val pts = List(Point(121.612389396, 31.2522492584), Point(121.612510129, 31.2522890327), Point(121.612447139, 31.2524226077), Point(121.612350552, 31.2523838254))
    //    路口2 对应的group.id="intersection02"  tablename="intersection02"
    //    val pts = List(Point(121.61205607, 31.2529267832), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612108563, 31.2528162074))
    //    道路road01 对应的group.id="road01"  tablename="road01"  CDJI
    val pts = List(Point(121.612447139, 31.2524226077), Point(121.612389397, 31.2524009617), Point(121.612247669,31.252842363), Point(121.612247669, 31.2528658129))
    //    车道lane01(直行北向) 对应的group.id="lane01"  tablename="lane01" COPI
    //    val pts = List(Point(121.612447139, 31.2524226077), Point(121.612418268, 31.2524117847), Point(121.612219323, 31.25285408795), Point(121.612247669, 31.2528658129))
    //    车道lane02(直行北向) 对应的group.id="lane02"  tablename="lane02"  DOPJ
    //    val pts = List(Point(121.612389397, 31.2524009617), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612190977, 31.252842363))

    //过滤出所有在车道内所有 Participant 对象
      var list: ArrayBuffer[Participant] = ArrayBuffer[Participant]()
      for (par <- parts) {
        //      取出当前 Participant对象的经纬度
        val longitude = par.location.longitude
        val latitude = par.location.latitude
        val flag = Meter.isInRoad(longitude, latitude,pts)
//        print("flag标签值为:"+flag)
        //      如果汽车在车道内 并且速度为0
        if (flag && par.speed.toInt == 0) list += par
//        if (flag) {
//          print("flag标签值为:"+flag)
//          list += par
//        }
      }
      //    遍历出所有汽车 与路口的距离 取出最大值 distance
      var distance = 0D
      val road_longitude = 121.612389397
      val road_latitude = 31.2524009617

      for (par <- list) {
        //      取出当前 Participant对象的经纬度
        val latitude = par.location.latitude
        val longitude = par.location.longitude
        //      计算出 当前Participant 与路口 点D的距离


//        val lon1 = 121.612389396
//        val lat1 = 31.2522492584
//        val lon2 = 121.612247669
//        val lat2 = 31.2528658129
//        println(getDistance(lon1, lat1, lon2, lat2))
        val dis = Meter.getDistance(longitude, latitude, road_longitude, road_latitude)
        //      如果当前距离
        print("dis:"+dis)
        if (dis > distance) distance = dis
      }
//    返回当前时间戳,排队长度 Tuple
    new QueueLength(t._2,distance,1)
}
}
