package hdpf.operator.fitter

import hdpf.bean.source.Participant
import org.apache.flink.api.common.functions.FilterFunction


/**
  * 判断点是否在多边形内部
  */

//GPS:lon:121.612389396  lat:31.2522492584
//UTM:   (X:1513126.13069 , Y:3506584.72927)
//GPS:lon:121.612510129  lat:31.2522890327
//UTM:   (X:1513137.28541 , Y:3506590.2989)
//GPS:lon:121.612447139  lat:31.2524226077
//UTM:   (X:1513129.78957 , Y:3506604.63275)
//GPS:lon:121.612389397  lat:31.2524009617
//UTM:   (X:1513124.4832 , Y:3506601.67595)
//GPS:lon:121.612350552  lat:31.2523838254
//UTM:   (X:1513120.94142 , Y:3506599.39923)
class IsInLane01 extends FilterFunction[Participant] {
  //算法
  case class Point(var x: Double, var y: Double) {
    def getPoint: Point = Point.apply(x, y)
  }
  override def filter(value: Participant): Boolean = {
    var flag=true
    val p = Point(value.location.longitude, value.location.latitude)


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

    var intersection = 0
    for (i <- pts.indices) {
      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)
      if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersection += 1
    }
    if (intersection % 2 == 1) {
      println(value.location)


      flag=true
      println("True:"+value.location)
    }

    else{
      flag=false
    }
    flag
  }
}
