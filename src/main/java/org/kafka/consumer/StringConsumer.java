package org.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 * Created by krishna on 08/11/17.
 */
class StringConsumer extends Thread{

    private final KafkaConsumer<String, String> consumer;
    private final BiFunction<String, String, Boolean> messageProcessor;

    private final List<String> topics;

    StringConsumer(String hosts, String groupId, List<String> topics, BiFunction<String, String, Boolean> messageProcessor) {
        this.messageProcessor = messageProcessor;
        Properties props = new Properties();
        props.put("bootstrap.servers", hosts);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        this.topics = topics;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(messageProcessor.apply(record.topic(), record.value()));
                }
            }
        } catch (WakeupException e) {
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
