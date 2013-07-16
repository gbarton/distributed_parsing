package com.gman.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class EmbeddedZK implements Runnable {
	private static final Log LOG = LogFactory.getLog(EmbeddedZK.class);
	private ServerCnxnFactory standaloneServerFactory = null;
	//just used to block against for initialization
	private ArrayBlockingQueue<Integer> port = new ArrayBlockingQueue<Integer>(1);
	private int zkPort = -1;
	File logDir = null;

	public EmbeddedZK() { }
	
	public int getPort() {
		if(zkPort == -1) {
			try {
				zkPort = port.poll(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("cant sit and wait for the port to be created apparently",e);
			}
		}
		return zkPort;
	}
	
	public String getURI() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostAddress() +":" + getPort();
	}
	

	public void shutdown() {
		LOG.info("embedded ZK shutting down");
		if(standaloneServerFactory != null)
			standaloneServerFactory.shutdown();
		if(logDir != null && logDir.exists())
			try {
				FileUtils.deleteDirectory(logDir);
			} catch (IOException e) {
				LOG.error("couldnt clean up tmp zk folder",e);
			}
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		final EmbeddedZK e = new EmbeddedZK();
		Thread t = new Thread(e);
		t.start();
		
		Thread.sleep(10000);
		System.out.println("stopping");
		
		e.shutdown();
		System.exit(0);

	}

	public void run() {
		int tickTime = 2000;
		int numConnections = 10;
		String dataDirectory = System.getProperty("java.io.tmpdir");

		logDir = new File(dataDirectory, "zookeeper-"+System.currentTimeMillis()).getAbsoluteFile();
		LOG.info("choosen ZK log dir: " + logDir.getAbsolutePath());

		ZooKeeperServer server;
		try {
			server = new ZooKeeperServer(logDir, logDir, tickTime);
//			ServerCnxnFactory standaloneServerFactory;
			standaloneServerFactory = ServerCnxnFactory.createFactory(0, numConnections);
			standaloneServerFactory.startup(server);
			int p = standaloneServerFactory.getLocalPort();
			port.add(p);
			LOG.info("Embedded zk up on port " + p);
		} catch (IOException e) {
			LOG.error("error starting embedded ZK",e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			LOG.error("error starting embedded ZK",e);
			throw new RuntimeException(e);
		}
	}

}
