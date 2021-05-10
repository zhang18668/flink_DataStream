package hdpfMQ.operator;

import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitSource extends RMQSource {

    public RabbitSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, RMQDeserializationSchema deliveryDeserializer) {
        super(rmqConnectionConfig, queueName, usesCorrelationId, deliveryDeserializer);
    }

}
