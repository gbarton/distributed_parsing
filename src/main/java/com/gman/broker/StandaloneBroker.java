package com.gman.broker;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;


import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

public class StandaloneBroker implements Runnable {
	private static final Log LOG = LogFactory.getLog(StandaloneBroker.class);

	private KafkaServer server;
	//just used to block against for initialization
	private ArrayBlockingQueue<Integer> port = new ArrayBlockingQueue<Integer>(1);
	private int zkPort;
	private String logDir = null;
	private int kPort = -1;
	
	public StandaloneBroker(int zkPort) {
		this.zkPort = zkPort;
	}
	
	public int getPort() {
		if(kPort == -1) {
			try {
				kPort = port.poll(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("cant sit and wait for the port to be created apparently",e);
			}
		}
		return kPort;
	}
	
	public void run() {
		if(port.size() == 0 || kPort == -1) {
			int tmpPort = NetUtils.getFreeSocketPort();
//			port = 9092;
			logDir = System.getProperty("java.io.tmpdir") 
					+ "/kafka-" + System.currentTimeMillis();
			LOG.info("logging to " + logDir);
			
			Properties props = new Properties();
			// dont set so it binds to all interfaces
			// props.setProperty("hostname", hostName);
			props.setProperty("port", tmpPort + "");
			props.setProperty("broker.id", "0");
			props.setProperty("log.dir", logDir);
			props.setProperty("num.partitions","2");
			props.setProperty("num.network.threads","2");
			props.setProperty("num.io.threads","2");
			// TODO: hardcode bad
			props.setProperty("zookeeper.connect", "localhost:"+zkPort);
			KafkaConfig kconf = new KafkaConfig(props);
	
			server = new KafkaServer(kconf, null);
			server.startup();
			port.add(tmpPort);
			LOG.info("Broker online");
		}
		
	}
	
	public void shutDown() {
		LOG.info("shutting down embedded broker");
		if(server != null)
			server.shutdown();
		if(logDir != null)
			try {
				FileUtils.deleteDirectory(new File(logDir));
			} catch (IOException e) {
				LOG.error("could not delete logs for kafka at: " + logDir);
			}
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		EmbeddedZK zk = new EmbeddedZK();
		
		Thread z = new Thread(zk);
		z.start();
//		Thread.sleep(500); //TODO: find a way to tell or block zk.port till initted
		StandaloneBroker broker = new StandaloneBroker(zk.getPort());
		Thread t = new Thread(broker);
		t.start();

		Thread.sleep(10000);
		System.out.println("kafka going down");
		broker.shutDown();
		System.exit(0);
		
		while(true) {
			Thread.sleep(10);
		}
	}

}
