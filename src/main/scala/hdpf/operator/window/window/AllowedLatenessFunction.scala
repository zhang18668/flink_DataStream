package hdpf.operator.window.window

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AllowedLatenessFunction extends ProcessWindowFunction[
  (String, Long, Int), (String, String, Int, String), String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long, Int)],
                       out: Collector[(String, String, Int, String)]): Unit = {

    // 是否被迟到数据更新
    val isUpdated = context.windowState.getState(
      new ValueStateDescriptor[Boolean]("isUpdated", Types.of[Boolean])
    )
    val count = elements.size
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    if (isUpdated.value() == false) {
      // 第一次使用process函数时， Boolean默认初始化为false，因此窗口函数第一次被调用时会进入这里
      out.collect((key, format.format(Calendar.getInstance().getTime), count, "first"))
      isUpdated.update(true)
    } else {
      // 之后isUpdated被置为true，窗口函数因迟到数据被调用时会进入这里
      out.collect((key, format.format(Calendar.getInstance().getTime), count, "updated"))
    }

  }
}
