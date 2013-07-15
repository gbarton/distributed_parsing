package com.gman.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.gman.util.Constants;


import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;

public class StandaloneBroker implements Runnable, Watcher {
	private static final Log LOG = LogFactory.getLog(StandaloneBroker.class);

	private KafkaServerStartable server;
	//just used to block against for initialization
	private ArrayBlockingQueue<Integer> port = new ArrayBlockingQueue<Integer>(1);
	private ArrayBlockingQueue<String> init = new ArrayBlockingQueue<String>(1);
	private String zkURI;
	private String logDir = null;
	private int kPort = -1;
	private ZooKeeper zk = null;
	private static String basePath = "/kafka-" + System.currentTimeMillis();
	
	//TODO: this should have the basepath folded in maybe?
	public StandaloneBroker(String zkURI) throws IOException {
		this.zkURI = zkURI;
		zk = new ZooKeeper(zkURI,3000,this);
	}

	@Override
	public void process(WatchedEvent event) {
    	try {
			zk.create(basePath,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info("zk broker store created: " + basePath);
		} catch (KeeperException e) {
			LOG.error("total fail with zk", e);
			throw new RuntimeException("cannot set the zk baseDir",e);
		} catch (InterruptedException e) {
			LOG.error("total fail with zk", e);
			throw new RuntimeException("cannot set the zk baseDir",e);
		} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {
				//gulp
			}
		}
    	init.add(basePath);
	}
	
	public int getPort() {
		if(kPort == -1) {
			try {
				kPort = port.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("cant sit and wait for the port to be created apparently",e);
			}
		}
		return kPort;
	}
	
	public String getURI() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostAddress() +":" + getPort();
	}
	
	public String getZKURI() throws UnknownHostException {
		return zkURI + basePath;
	}
	
	public void run() {
		if(port.size() == 0 || kPort == -1) {
			//TODO: do this better
			try {
				init.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("timed out waiting for zk dir to init");
			}
			LOG.info("*************** Starting Broker INIT");
			int tmpPort = NetUtils.getFreeSocketPort();
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
			props.setProperty("zookeeper.connect", zkURI+basePath);
			KafkaConfig kconf = new KafkaConfig(props);
	
			server = new KafkaServerStartable(kconf);
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
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws InterruptedException, UnknownHostException, IOException {
		EmbeddedZK zk = new EmbeddedZK();
		
		Thread z = new Thread(zk);
		z.start();
//		Thread.sleep(500); //TODO: find a way to tell or block zk.port till initted
		StandaloneBroker broker = new StandaloneBroker(zk.getURI());
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
