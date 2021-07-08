package hdpf.operator.map

import hdpf.bean.source.{LaneCar, Participant}
import org.apache.flink.api.common.functions.MapFunction
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}


class JOSNFunction() extends MapFunction[LaneCar, String] {

  override def map(car: LaneCar): String = {
      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

      write(car)
}
}
