package hdpf.operator

import hdpf.bean.Participant
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AllWindowApply extends AllWindowFunction[Participant,  Int, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Participant], out: Collector[Int]): Unit = {
    val set = Set[Participant]()
    while(input.iterator.hasNext){
      val participant: Participant = input.iterator.next()
      set.+(participant.id)
    }
    out.collect(set.toArray.length)
  }
}
