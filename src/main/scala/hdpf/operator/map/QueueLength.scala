package hdpf.operator.map

import hdpf.bean.{Device, Participant}
import hdpf.utils.Meter
import hdpf.utils.Meter.Point
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable.ArrayBuffer
class QueueLength extends MapFunction[Device] {
  override def map(t: Device): Double = {
    //    将所有的汽车Participant 对象集合取出
    val parts: Array[Participant] = t.`object`
    //    车道lane02(直行北向) 对应的group.id="lane02"  tablename="lane02"  DOPJ
    val pts = List(Point(121.612389397, 31.2524009617), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612190977, 31.252842363))
    //过滤出所有在车道内所有 Participant 对象
    var list:ArrayBuffer[Participant] =ArrayBuffer[Participant]()
    for (par <- parts) {
      //      取出当前 Participant对象的经纬度
      val latitude = par.location.latitude
      val longitude = par.location.longitude
      val flag = Meter.isInRoad(latitude, longitude, pts)
//      如果汽车在车道内 并且速度为0
      if(flag && par.speed==0) list+=par
    }
    //    遍历出所有汽车 与路口的距离 取出最大值 distance
    var distance = 0D
    val road_latitude = 121.612447139
    val road_longitude = 31.2524226077

    for (par <- list) {
      //      取出当前 Participant对象的经纬度
      val latitude = par.location.latitude
      val longitude = par.location.longitude
//      计算出 当前Participant 与路口 点D的距离
      val dis = Meter.getDistance(latitude, longitude, road_latitude, road_longitude)
//      如果当前距离
      if (dis > distance) distance = dis
    }
    distance
  }
}
