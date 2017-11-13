package org.kafka.producer;

/**
 * Created by krishna on 13/11/17.
 */
public class Producer {

    private final StringProducer stringProducer;

    public Producer(String hosts, String type) throws Exception {
        if(!"string".equals(type)){
            throw new Exception("unsupported type");
        }
        this.stringProducer = new StringProducer(hosts);
    }

    public void sendMessage(String topic, String message){
        stringProducer.publishMessage(topic, message);
    }
}
