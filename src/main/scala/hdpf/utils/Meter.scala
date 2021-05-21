package hdpf.utils

import hdpf.bean.enity.Point

/**
  * 计算GPS经纬度 俩点之间的直线距离
  */
object Meter {
  private val EARTH_RADIUS: Double = 6378.137

  private def rad(d: Double): Double = d * Math.PI / 180.0

  /**
    * 通过经纬度判断是否在路口道路上停车
    *
    * @param latitude
    * @param longitude
    * @return Boolean
    */
  def isInRoad(longitude: Double, latitude: Double, pts: List[Point]): Boolean = {
    var flag = false
    val p = Point(longitude, latitude)
    var intersection = 0
    for (i <- pts.indices) {
      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)
      if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersection += 1
    }
    if (intersection % 2 == 1) {
      flag = true
      println("True:" + p)
    }
    else {
      flag = false
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