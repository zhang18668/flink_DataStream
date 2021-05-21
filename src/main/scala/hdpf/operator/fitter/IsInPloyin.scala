package hdpf.operator.fitter

import hdpf.bean.enity.Point
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


class IsInPloyin(pts:List[Point]) extends FilterFunction[Participant] {
  //算法
  override def filter(value: Participant): Boolean = {
    var flag=true
    val p = Point(value.location.longitude, value.location.latitude)
    var intersection = 0
    for (i <- pts.indices) {
      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)
      if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersection += 1
    }
    if (intersection % 2 == 1) {
      flag=true
    }
    else{
      flag=false
    }
    flag
  }
}
