package hdpf.operator


/**
  * 计算GPS经纬度 俩点之间的直线距离
  */
object Meter {
  def main(args: Array[String]): Unit = {
    //    val pts = List(Point(121.612389396, 31.2522492584), Point(121.612510129, 31.2522890327), Point(121.612447139, 31.2524226077), Point(121.612350552, 31.2523838254))
    //    val pts = List(Point(121.612447139, 31.2524226077), Point(121.612389397, 31.2524009617), Point(121.612247669,31.252842363), Point(121.612247669, 31.2528658129))

    val lon1 = 121.612389396
    val lat1 = 31.2522492584
    val lon2 = 121.612247669
    val lat2 = 31.2528658129
    println(getDistance(lon1, lat1, lon2, lat2))
  }


  private val EARTH_RADIUS: Double = 6378.137

  private def rad(d: Double): Double = d * Math.PI / 180.0

  case class Point(var x: Double, var y: Double) {
    def getPoint: Point = Point.apply(x, y)
  }

  /**
    * 通过经纬度判断是否在路口道路上停车
    *
    * @param latitude
    * @param longitude
    * @return Boolean
    */
  def isInRoad(latitude: Double, longitude: Double,pts:List[Point]): Boolean = {
    var flag = false
    val p = Point(latitude, longitude)

    var intersection = 0
    for (i <- pts.indices) {

      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)

      if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersection += 1
    }
    if (intersection % 2 == 1) {
      //      println(value.location)


      flag = true
    } else {
      flag = false
      //      println("false:"+value.location)
    }
    flag
  }


  /**
    * 通过经纬度获取距离(单位：米)
    *
    * @param lat1
    * @param lng1
    * @param lat2
    * @param lng2
    * @return 距离(单位：米)
    */
  def getDistance(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double = {
    val radLat1: Double = rad(lat1)
    val radLat2: Double = rad(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = rad(lng1) - rad(lng2)
    var s: Double = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    s = s * EARTH_RADIUS
    s = s * 10000d.round / 10000d
    s = s * 1000
    s
  }

}