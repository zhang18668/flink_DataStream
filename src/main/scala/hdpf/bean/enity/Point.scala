package hdpf.bean.enity

case class Point(var x: Double, var y: Double) {
  def getPoint: Point = Point.apply(x, y)
}
