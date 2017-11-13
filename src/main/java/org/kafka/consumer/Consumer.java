package org.kafka.consumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

/**
 * Created by krishna on 08/11/17.
 */
public class Consumer {

    private final ExecutorService threadPool;

    public Consumer(String type, ExecutorService threadPool) throws Exception {
        if (type==null || "string".equals(type.toLowerCase())){
            throw new Exception("Unsupported type");
        }
        this.threadPool = threadPool;
    }

    public Consumer(String type, Integer threads) throws Exception {
        if (type==null || "string".equals(type.toLowerCase())){
            throw new Exception("Unsupported type");
        }
        this.threadPool = Executors.newFixedThreadPool(threads);
    }

    public void start(String hosts, String groupId, List<String> topics, BiFunction<String, String, Boolean> processor){
        threadPool.submit(new StringConsumer(hosts, groupId, topics, processor));
    }
}
