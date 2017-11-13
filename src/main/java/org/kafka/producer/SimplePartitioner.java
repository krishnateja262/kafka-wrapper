package org.kafka.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by krishna on 13/11/17.
 */
public class SimplePartitioner implements Partitioner{

    public SimplePartitioner(VerifiableProperties props) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        int partition = Integer.parseInt((String)key);
        if(partition >= numPartitions){
            return partition % numPartitions;
        }
        return partition;
    }
}
