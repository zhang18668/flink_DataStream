package hdpf.operator

import hdpf.bean.Participant
import org.apache.flink.api.common.functions.FilterFunction

//算法
case class Point(var x: Double, var y: Double) {
  def getPoint: Point = Point.apply(x, y)
}

/**
 * 判断点是否在多边形内部
 */
class IsInPloyin extends FilterFunction[Participant] {
  override def filter(value: Participant): Boolean = {
    val p = Point(value.location.longitude, value.location.latitude)
    val pts = List(Point(114.0717, 30.44909), Point(114.0717, 30.44910), Point(114.0722, 30.44909), Point(114.0722, 30.44910))

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
