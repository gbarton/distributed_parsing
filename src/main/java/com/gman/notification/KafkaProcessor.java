package com.gman.notification;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaProcessor extends BaseProcessor {
	private static final Log LOG = LogFactory.getLog(KafkaProcessor.class);
	//I still dont know why consumer wants zk and producer wants brokers
	private String brokerURI;
	private String BrokerZKURI;
	private final ConsumerConnector consumer;

	public KafkaProcessor(String broker, String zkuri) {
		this.brokerURI = broker;
		this.BrokerZKURI = zkuri;
		
		Properties props = new Properties();
        props.put("zookeeper.connect", zkuri);
        //TODO: do i need different group names?
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig kconf = new ConsumerConfig(props);
		consumer = Consumer.createJavaConsumerConnector(kconf);
	}
	
	@Override
	public void sendControlEvent(WorkUnit work, Control c) {
		System.out.println("controlEvent: " + c + " unit: " + work);
	}
	
	@Override
	public Control getControlEvent() {
		return null;
	}
	
	@Override
	public void createdNewWork(WorkUnit from, String path) {
		System.out.println(from + " generated child: " + path);
	}
	
	@Override
	public WorkUnit getNextWorkUnit() {
		return null;
	}

	@Override
	public void run() {
		
	}
}
