package hdpf.operator.map

import hdpf.bean.enity.Point
import hdpf.bean.sink.QueueLength
import hdpf.bean.source.{Device, Participant}
import hdpf.utils.Meter
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable.ArrayBuffer

class QueueLengthFunction(value:(List[Point],Int,Int)) extends MapFunction[(Device, String),QueueLength] {
  override def map(t: (Device, String)): QueueLength ={
      //    将所有的汽车Participant 对象集合取出
      val parts: Array[Participant] = t._1.`object`
    //过滤出所有在车道内所有 Participant 对象
      var list: ArrayBuffer[Participant] = ArrayBuffer[Participant]()
      for (par <- parts) {
        //      取出当前 Participant对象的经纬度
        val longitude = par.location.longitude
        val latitude = par.location.latitude
        val flag = Meter.isInRoad(longitude, latitude,value._1)
        //      如果汽车在车道内 并且速度为0,将汽车加入集合
        if (flag && par.speed.toInt == 0) list += par
      }
      //    遍历出所有汽车 与路口的距离 取出最大值 distance
      var distance = 0D
//      val road_longitude = 121.612389397
      val road_longitude = value._1(value._3).x
      val road_latitude = value._1(value._3).y
//      val road_latitude = 31.2524009617

      for (par <- list) {
        //      取出当前 Participant对象的经纬度
        val latitude = par.location.latitude
        val longitude = par.location.longitude
        //      计算出 当前Participant 与路口 点D的距离
        val dis = Meter.getDistance(longitude, latitude, road_longitude, road_latitude)
        //      如果当前距离大于最大距离 取当前距离为排队长度
        if (dis > distance) distance = dis
      }
//    返回当前时间戳,排队长度 Tuple
    new QueueLength(t._2,distance,value._2)
}
}
