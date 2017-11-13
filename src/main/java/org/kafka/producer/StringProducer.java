package org.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

class StringProducer {

    private static Producer<String, String> producer;

    Long counter = 0l;

    StringProducer(String brokerList) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "org.kafka.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<>(config);
    }

    void publishMessage(String topicName, String message) {
        counter = counter + 1;
        KeyedMessage<String, String> data = new KeyedMessage(topicName, String.valueOf(counter), message);
        if (counter > 100000) {
            counter = 0l;
        }
        producer.send(data);
    }
}
