package hdpf.operator

import hdpf.bean.Participant
import org.apache.flink.api.common.functions.RichFilterFunction

//算法
case class Point(var x: Double, var y: Double) {
  def getPoint: Point = Point.apply(x, y)
}

/**
  * 判断点是否在多边形内部
  */
class IsInPloyin extends RichFilterFunction[Participant] {
  override def filter(value: Participant): Boolean = {
    val p = Point(value.location.longitude, value.location.latitude)
    val pts = List(Point(121.612389396, 29.2522492584), Point(121.612510129, 36.2522890327), Point(111.612447139, 29.2524009617), Point(111.612350552, 36.2523838254))

    var intersection = 0
    for (i <- pts.indices) {

      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)

      if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersection += 1
    }
    if (intersection % 2 == 1) true else false

  }
}
