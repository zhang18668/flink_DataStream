package hdpf.constant

import hdpf.bean.enity.Point


object Constant {

  //    路口1 对应的group.id="intersection01"  tablename="intersection01"
  val pts1 = List(Point(121.612389396, 31.2522492584), Point(121.612510129, 31.2522890327), Point(121.612447139, 31.2524226077), Point(121.612350552, 31.2523838254))
  val roadId1 = 1
  val index1 = 1
  val valueTuple1: (List[Point], Int, Int) =(pts1,roadId1,index1)

  //    路口2 对应的group.id="intersection02"  tablename="intersection02"
  val pts2 = List(Point(121.61205607, 31.2529267832), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612108563, 31.2528162074))

  val roadId2 = 2
  val index2 = 1
  val valueTuple2: (List[Point], Int, Int) =(pts2,roadId2,index2)

  //    道路road01 对应的group.id="road01"  tablename="road01"  CDJI
  val pts3 = List(Point(121.612447139, 31.2524226077), Point(121.612389397, 31.2524009617), Point(121.612247669, 31.252842363), Point(121.612247669, 31.2528658129))

  val roadId3 = 3

  val index3= 1
  val valueTuple3: (List[Point], Int, Int) =(pts3,roadId3,index3)
  //    车道lane01(直行北向) 对应的group.id="lane01"  tablename="lane01" COPI
  val pts4 = List(Point(121.612447139, 31.2524226077), Point(121.612418268, 31.2524117847), Point(121.612219323, 31.25285408795), Point(121.612247669, 31.2528658129))
  val roadId4 = 3
  val index4= 1
  val valueTuple4: (List[Point], Int, Int) =(pts4,roadId4,index4)
  //    车道lane02(直行北向) 对应的group.id="lane02"  tablename="lane02"  DOPJ
  val pts5 = List(Point(121.612389397, 31.2524009617), Point(121.612196226, 31.252972781), Point(121.612247669, 31.2528658129), Point(121.612190977, 31.252842363))

  val roadId5= 3
  val index5= 1
  val valueTuple5: (List[Point], Int, Int) =(pts5,roadId5,index5)

  def main(args: Array[String]): Unit = {
    println(pts1(1).x)
  }
}
