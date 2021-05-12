package test

import scala.collection.mutable.ArrayBuffer

object demo {
  def main(args: Array[String]): Unit = {
    var arr = new ArrayBuffer[Int]()
    arr=ArrayBuffer(1,2,3,2,2,2,5,4,2)
    val intToInt: Map[Int, Int] = arr.map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.size)).filter(x => x._2 > arr.length / 6)
  print(intToInt.size)
  }
}
