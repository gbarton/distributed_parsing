package com.gman.broker;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * bout as simple as it gets
 */
public class SimplePartitioner implements Partitioner<String> {
    public SimplePartitioner (VerifiableProperties props) {

    }
    
    //TODO: null check??
    public int partition(String key, int numPartitions) {
    	return (key.hashCode()) % numPartitions;
  }

}