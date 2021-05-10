package hdpf.operator

import hdpf.bean.{Device, Participant, Payload}
import org.apache.flink.api.common.functions.MapFunction
import hdpf.operator.Meter
import hdpf.operator.Meter.Point

class QueueLength extends MapFunction[Device] {
  override def map(t: Device): Double = {
    //    将所有的汽车Participant 对象集合取出
    val parts: Array[Participant] = t.`object`
    //    车道lane02(直行北向) 对应的group.id="lane02"  tablename="lane02"  DOPJ
    val pts = List(Point(121.612389397, 31.2524009617), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612190977, 31.252842363))


    //    遍历出所有汽车 与路口的距离 取出最大值 distance
    var distance = 0D
    val road_latitude = 121.612447139
    val road_longitude = 31.2524226077
    //过滤出所有在车道内所有 Participant 对象
    for (par <- parts) {
      Meter.isInRoad()
    }
    for (par <- parts) {
      //      取出当前 Participant对象的经纬度
      val latitude = par.location.latitude
      val longitude = par.location.longitude
//      计算出 当前Participant 与路口 点C的距离
      val dis = Meter.getDistance(latitude, longitude, road_latitude, road_longitude)
//      如果当前距离
      if (dis > distance) distance = dis
    }
    distance
  }
}
