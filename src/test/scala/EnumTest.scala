

/**
  * Created by szh on 2018/12/21.
  */
object EnumTest extends Enumeration {

  type EnumTest = Value //声明枚举对外暴露的变量类型

  val CC, CC2, CC3 = Value

  val Q1 = Value(100, "Quarter1")
  val Q2 = Value(200, "Quarter2")


  def main(args: Array[String]): Unit = {

    for (x <- EnumTest.values) {
      println(s"${x.id}  ${x}")
    }

    println(EnumTest.Q1)

    println(EnumTest.CC)
  }
}