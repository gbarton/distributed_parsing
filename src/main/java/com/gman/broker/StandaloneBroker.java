package com.gman.broker;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

public class StandaloneBroker implements Runnable {
	private static final Log LOG = LogFactory.getLog(StandaloneBroker.class);

	private KafkaServer server;
	private int port = -1;
	private AtomicInteger done;
	
	public StandaloneBroker(AtomicInteger done) {
		this.done = done;
	}
	
	public int getPort() {
		return port;
	}
	
	public void run() {
		if(port == -1) {
//			port = NetUtils.getFreeSocketPort();
			port = 9092;
			String randId = System.currentTimeMillis() + "";
			Properties props = new Properties();
			// dont set so it binds to all interfaces
			// props.setProperty("hostname", hostName);
			props.setProperty("port", port + "");
			props.setProperty("broker.id", "0");
//			props.setProperty("log.dir", "/tmp/embeddedkafka/" + randId);
			props.setProperty("num.partitions","2");
			props.setProperty("num.network.threads","2");
			props.setProperty("num.io.threads","2");
			// TODO: hardcode bad
			props.setProperty("zookeeper.connect", "localhost:2181/kafkatest");
			KafkaConfig kconf = new KafkaConfig(props);
	
			server = new KafkaServer(kconf, null);
			server.startup();
			LOG.info("Broker online");
		}
		
		while(true) {
			if(done.get() == 0)
				break;
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
		
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		AtomicInteger done = new AtomicInteger(1);

		Thread t = new Thread(new StandaloneBroker(done));
		t.start();

		while(true) {
			Thread.sleep(10);
			
		}
		
		
	}

}
