package com.gman.example;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.gman.util.Constants;
import com.gman.util.IOUtils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.utils.SystemTime;
import kafka.utils.Time;

public class BrokerServer {
	private static final Log LOG = LogFactory.getLog(BrokerServer.class);
	private KafkaServerStartable server;
	private ContainerId containerId;
	// Configuration
	private Configuration conf;

	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;

	String hostName;
	int port;

	FileSystem fs;

	public BrokerServer() throws IOException {
		LOG.info("***********************");
		LOG.info("BrokerServer comming up");
		LOG.info("***********************");
		conf = new YarnConfiguration();
		Map<String, String> envs = System.getenv();
		conf.set("fs.defaultFS", envs.get(Constants.ENV_HDFS_URI));
		rpc = YarnRPC.create(conf);
		for (String key : envs.keySet())
			LOG.info("ENV PARAM: " + key + " " + envs.get(key));
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		if (containerIdString == null) {
			// container id should always be set in the env by the framework
			throw new IllegalArgumentException("ContainerId not set in the environment");
		}

		hostName = envs.get(ApplicationConstants.NM_HOST_ENV);
		port = NetUtils.getFreeSocketPort();

		Properties props = new Properties();
		// dont set so it binds to all interfaces
		// props.setProperty("hostname", hostName);
		props.setProperty("port", port + "");
		props.setProperty("broker.id", "1");
		props.setProperty("log.dir", "/tmp/embeddedkafka/" + envs.get(Constants.ENV_NAME));
		props.setProperty("zookeeper.connect",envs.get(Constants.BROKER_ZK_URI));
		// TODO: hardcode bad
//		props.setProperty("zookeeper.connect", "localhost:2181/" + envs.get(Constants.ENV_NAME));
		KafkaConfig kconf = new KafkaConfig(props);

		server = new KafkaServerStartable(kconf);
		server.startup();

		LOG.info("should have a kafka server up on port: " + port);
		fs = FileSystem.get(conf);

		Path p = new Path(IOUtils.makeAbsolutePath(envs.get(Constants.ENV_NAME),
				"kafkabroker",
				hostName, port + ""));
		LOG.info("Path picked to write to HDFS: " + p.toString());
		FSDataOutputStream outputStream = fs.create(p);
		outputStream.write(port);
		outputStream.close();

		LOG.info("wrote to hdfs my kafka port to: " + p.toString());
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		BrokerServer server = new BrokerServer();

		// TODO: will have to do something better than this
		while (true)
			Thread.sleep(100);

	}

}
