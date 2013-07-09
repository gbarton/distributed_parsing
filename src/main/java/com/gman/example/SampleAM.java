package com.gman.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.gman.util.Constants;
import com.gman.util.IOUtils;
import com.gman.util.YarnUtils;

public class SampleAM implements Watcher {

	private static final Log LOG = LogFactory.getLog(SampleAM.class);
	
	private boolean initComplete = false;

	private ContainerId containerId;
	private ApplicationAttemptId appAttemptID;
	// Configuration
	private Configuration conf;
	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;
	private AMRMProtocol resourceManager;
	Map<String, String> envs;

	private String host;
	private int port;
	// cluster stats
	private Resource min;
	private Resource max;

	private ZooKeeper zk;

	public SampleAM() throws IOException {
		LOG.info("*******************");
		LOG.info("SampleAM comming up");
		LOG.info("*******************");
		envs = System.getenv();

		conf = new YarnConfiguration();
		conf.set("fs.defaultFS", envs.get(Constants.ENV_HDFS_URI));
		rpc = YarnRPC.create(conf);
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		if (containerIdString == null) {
			// container id should always be set in the env by the framework
			throw new IllegalArgumentException("ContainerId not set in the environment");
		}
		for (String key : envs.keySet())
			LOG.info("ENV PARAM: " + key + " " + envs.get(key));

		containerId = ConverterUtils.toContainerId(containerIdString);
		appAttemptID = containerId.getApplicationAttemptId();

		host = envs.get(ApplicationConstants.NM_HOST_ENV);
		port = NetUtils.getFreeSocketPort();
		
		LOG.info("connecting to zookeeper: " + envs.get(Constants.ENV_ZK_URI));
		zk = new ZooKeeper(envs.get(Constants.ENV_ZK_URI),3000,this);
	}

	/**
	 * Really this is just here to create the directory we will bind our brokers to
	 * and add it into the env vars for passing down into the containers.
	 */
    public void process(WatchedEvent event) {
    	String path = "/" + envs.get(Constants.ENV_NAME);
    	LOG.info("zookeeper connection complete, creating path: " + path);
    	try {
			zk.create(path,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info("zk broker store created");
		} catch (KeeperException e) {
			LOG.error("total fail with zk", e);
			System.exit(1);
		} catch (InterruptedException e) {
			LOG.error("total fail with zk", e);
			System.exit(1);
		} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {
				//gulp
			}
		}
    	//go ahead and add to the constants where to bind the brokers to
    	envs.put(Constants.BROKER_ZK_URI, envs.get(Constants.ENV_ZK_URI)+path);
    	initComplete = true;
    }
	
	public void register() throws YarnRemoteException, InterruptedException {
		while(!initComplete)
			Thread.sleep(1000);
		
		// Connect to the Scheduler of the ResourceManager.
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
		System.out.println("Connecting to ResourceManager at " + rmAddress);
		resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);

		RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
		appMasterRequest.setApplicationAttemptId(appAttemptID);
		appMasterRequest.setHost(host);

		LOG.info("registring");
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterRequest);
		min = response.getMinimumResourceCapability();
		max = response.getMaximumResourceCapability();
		LOG.info("registered");
	}

	public void start() throws IOException, InterruptedException {
		LOG.info("start beginning");

		AllocateRequest req = Records.newRecord(AllocateRequest.class);

//		// Resource Request
//		ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);
//
//		// to data locality TODO: will have to work that out eventually
//		rsrcRequest.setHostName("*");
//		Priority pri = Records.newRecord(Priority.class);
//		pri.setPriority(1);
//		rsrcRequest.setPriority(pri);
//
//		LOG.info("ram requesting: " + min.getMemory());
//		// Set up resource type requirements for memory
//		Resource capability = Records.newRecord(Resource.class);
//		capability.setMemory(min.getMemory());
//		rsrcRequest.setCapability(capability);
//
//		// set no. of containers needed matching the specifications
//		rsrcRequest.setNumContainers(1);
//
//		// Add the list of containers being asked for
//		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
//		requestedContainers.add(rsrcRequest);
//		req.addAllAsks(requestedContainers);

		// Resource Request
		// Add the list of containers being asked for
		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
		requestedContainers.add(YarnUtils.getResRequest(min, max, min.getMemory(), 1));
		req.addAllAsks(requestedContainers);

		List<ContainerId> releasedContainers = new ArrayList<ContainerId>();

		// TODO: make enum
		int rmRequestID = 1;

		req.setResponseId(rmRequestID);

		// Set ApplicationAttemptId
		req.setApplicationAttemptId(appAttemptID);

		req.addAllReleases(releasedContainers);

		// Assuming the ApplicationMaster can track its progress
		req.setProgress(0.1f);
		AllocateResponse allocateResponse;

		String fileName = System.getenv(Constants.ENV_JAR);
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/" + fileName);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(p, true);
		while (files.hasNext()) {
			LocatedFileStatus fileStatus = files.next();
			LOG.info("path matched file: " + fileStatus.getPath().toString());
		}

		// loop until we get a container to startup in.
		// TODO: need a little util to go fetch containers
		int numBrokers = 0;
		while (numBrokers < 1) {
			LOG.info("resource try #" + rmRequestID);
			req.setResponseId(rmRequestID);
			// fire initial request for resources, in this case 1 container.
			allocateResponse = resourceManager.allocate(req);
			LOG.info("allocate response sent: " + req.getResponseId());

			// Get AMResponse from AllocateResponse
			AMResponse amResp = allocateResponse.getAMResponse();
			LOG.info("response received: " + amResp.getResponseId());
			rmRequestID++;

			// tracking
//			List<ContainerId> cId = new ArrayList<ContainerId>();

			// Retrieve list of allocated containers from the response
			// and on each allocated container, lets assume we are launching
			// the same job.
			List<Container> allocatedContainers = amResp.getAllocatedContainers();
			LOG.info("containers given to AM: " + allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
				LOG.info("Launching command on a new container." + ", containerId=" + allocatedContainer.getId()
						+ ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":"
						+ allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
						+ allocatedContainer.getNodeHttpAddress() + ", containerState=" + allocatedContainer.getState()
						+ ", containerResourceMemory=" + allocatedContainer.getResource().getMemory());
//				cId.add(allocatedContainer.getId());
				createContainer(allocatedContainer, null, "com.gman.example.BrokerServer", min.getMemory(), fileName,
						fs.getFileStatus(p), null);
				numBrokers++;

				// status the container
				GetContainerStatusRequest statusReq = Records.newRecord(GetContainerStatusRequest.class);
				statusReq.setContainerId(allocatedContainer.getId());
				ContainerManager cm = getCM(allocatedContainer, conf);
				GetContainerStatusResponse statusResp = cm.getContainerStatus(statusReq);
				LOG.info("Container Status" + ", id=" + allocatedContainer.getId().toString() + ", status="
						+ statusResp.getStatus());
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			if (allocatedContainers.size() > 0)
//				break;
		}

		// at this point i should have a kafka server up, read hdfs to find out
		// where its located.
		String brokerURI = "";
		Path brokerBasePath = new Path(IOUtils.makeAbsolutePath(envs.get(Constants.ENV_NAME), "kafkabroker"));
		while(true) {
			LOG.info("waiting for brokers to register");
			Thread.sleep(1000);
			if(fs.exists(brokerBasePath))
				break;
		}
		//TODO: overuse of sleepings
		Thread.sleep(1000);
		LOG.info("got at least one broker written out now.");

		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(brokerBasePath,true);
		while(listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			String path = status.getPath().toString();
			LOG.info("inspecting path: " + path);
			//we want the last 2 / to change it into a : for port
			path = path.substring(path.lastIndexOf("/",path.lastIndexOf("/")-1)+1);
			LOG.info("last part of path: " + path);
			if(!"".equals(brokerURI))
				brokerURI+=",";
			brokerURI+=path.replace("/", ":");
		}
		
		LOG.info("found my brokers: " + brokerURI);
		
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerURI);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.gman.broker.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		LOG.info("producer created");
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(Constants.DIS_TOPIC, Constants.DIS_SERVICE, "value1");

		producer.send(data);
		LOG.info("wrote message: " + data);
		
	}

	// TODO: should be a nice util to have this method
	private List<Container> getContainers(AMRMProtocol rm, AllocateRequest req, int numContainers) {
		return null;
	}

	/**
	 * Connects to the given containers manager (the nodemanager really I think)
	 * 
	 * @param c Container to lookup
	 * @param conf
	 * @return
	 */
	private ContainerManager getCM(Container c, Configuration conf) {
		// Connect to ContainerManager on the allocated container
		String cmIpPortStr = c.getNodeId().getHost() + ":" + c.getNodeId().getPort();
		InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
		ContainerManager cm = (ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf);
		return cm;
	}

	private void createContainer(Container container, ApplicationId appId, String appName, int amMemory,
			String jarName, FileStatus fileStatus, String[] args) throws YarnRemoteException {
		ContainerManager cm = getCM(container, conf);

		// Now we setup a ContainerLaunchContext
		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

		ctx.setContainerId(container.getId());
		ctx.setResource(container.getResource());

		try {
			ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
		} catch (IOException e) {
			LOG.error("Getting current user failed when trying to launch the container", e);
		}

		// Set the environment, hopefully everythings been past down from
		// AppMan.
		Map<String, String> env = new HashMap<String, String>();
		Constants.fill(System.getenv(), env);
		// i think this shoulda been done FOR me
		env.put(ApplicationConstants.AM_CONTAINER_ID_ENV, ConverterUtils.toString(container.getId()));
		env.put(ApplicationConstants.NM_HOST_ENV, container.getNodeId().getHost());
		ctx.setEnvironment(env);

		// Set the local resources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LocalResource shellRsrc = Records.newRecord(LocalResource.class);
		shellRsrc.setType(LocalResourceType.FILE);
		shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		shellRsrc.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()));
		shellRsrc.setTimestamp(fileStatus.getModificationTime());
		shellRsrc.setSize(fileStatus.getLen());

		localResources.put(jarName, shellRsrc);

		ctx.setLocalResources(localResources);

		// Construct the command to be executed on the launched container
		// String command = "echo CLASSPATH=$CLASSPATH GCP=$(hadoop classpath)"
		String command = "${JAVA_HOME}" + "/bin/java -Xmx" + amMemory + "m " + appName + " 1>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out" + " 2>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err";

		List<String> commands = new ArrayList<String>();
		commands.add(command);
		ctx.setCommands(commands);

		// Send the start request to the ContainerManager
		StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
		startReq.setContainerLaunchContext(ctx);
		cm.startContainer(startReq);
		LOG.info("request sent to start broker");
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		SampleAM am = new SampleAM();
		am.register();
		am.start();
		while (true)
			Thread.sleep(100);
	}

}
