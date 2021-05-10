package hdpf.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class RabbitMQ_source extends RichSourceFunction{

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def run(sourceContext: SourceFunction.SourceContext[Nothing]): Unit = ???

  override def cancel(): Unit = ???
}
