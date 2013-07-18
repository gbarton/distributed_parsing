package com.eqt.dp.am;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.gman.broker.StandaloneBroker;
import com.gman.util.Constants;
import com.gman.util.YarnUtils;

public class YarnAppManager implements Watcher {
	private static final Log LOG = LogFactory.getLog(YarnAppManager.class);
	private AtomicBoolean init = new AtomicBoolean(false);
	
	//override to make the brokers external to the AM, this also will make them
	//redundant and all that stuffs.
	//TODO: make that work :)
	protected boolean embeddedBroker = true;
	private StandaloneBroker broker = null;

	private ZooKeeper zk;
	private ContainerId containerId;
	private ApplicationAttemptId appAttemptID;
	// Configuration
	private Configuration conf;
	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;
	private AMRMProtocol resourceManager;
	Map<String, String> envs = new HashMap<String, String>();
	
	//info about where we are running
	private String host;
//	private int port;
	
	// cluster stats
	private Resource min;
	private Resource max;
	
	public YarnAppManager() throws IOException {
		LOG.info("*******************");
		LOG.info("YarnAppManager comming up");
		LOG.info("*******************");
		envs.putAll(System.getenv());

		conf = new YarnConfiguration();
//		conf.set("fs.defaultFS", envs.get(Constants.ENV_HDFS_URI));
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
//		port = NetUtils.getFreeSocketPort();
		
		LOG.info("connecting to zookeeper: " + envs.get(Constants.ENV_ZK_URI));
		zk = new ZooKeeper(envs.get(Constants.ENV_ZK_URI),3000,this);
		LOG.info("spinning until initialization complete.");
		while(!init.get())
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				LOG.warn("trouble sleeping.");
			}
		
		LOG.info("completed ZK initialization");
		
		// Connect to the Scheduler of the ResourceManager.
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
		System.out.println("Connecting to ResourceManager at " + rmAddress);
		resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);

		RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
		appMasterRequest.setApplicationAttemptId(appAttemptID);
		appMasterRequest.setHost(host);

		if(embeddedBroker) {
			LOG.info("setting up enbedded broker.");
			broker = new StandaloneBroker(envs.get(Constants.BROKER_ZK_URI));
			Thread tb = new Thread(broker,"broker");
			tb.start();
			envs.put(Constants.BROKER_URI, broker.getURI());
			LOG.info("broker online at port: " + broker.getPort());
		}
		
		LOG.info("registring");
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterRequest);
		min = response.getMinimumResourceCapability();
		max = response.getMaximumResourceCapability();
		LOG.info("registered");
		
	}

	@Override
	public void process(WatchedEvent event) {
		//TODO: when we go to multiple brokers this has to create more than 1 dir
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
    	init.set(true);
	}
	
	/**
	 * Use this for consumers
	 * @return
	 */
	protected String getBrokerZKURI() {
		return envs.get(Constants.BROKER_ZK_URI);
	}
	
	/**
	 * use this for producers
	 * @return
	 */
	protected String getBrokerURI() {
		return envs.get(Constants.BROKER_URI);
	}
	
	public List<Container> newContainers(int numContainers, int memory, int maxRequestAttempts) throws IOException {
		
		List<Container> allocatedContainers = new ArrayList<Container>();
		
		AllocateRequest req = Records.newRecord(AllocateRequest.class);
		// Resource Request
		// Add the list of containers being asked for
		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
		requestedContainers.add(YarnUtils.getResRequest(min, max, memory, numContainers));
		req.addAllAsks(requestedContainers);

		//request ID AND number of attempts all in 1! so nice.
		int rmRequestID = 0;

		// Set ApplicationAttemptId
		req.setApplicationAttemptId(appAttemptID);
		//none to release in this case.
		List<ContainerId> releasedContainers = new ArrayList<ContainerId>();
		req.addAllReleases(releasedContainers);
		req.setProgress(0.1f);
		AllocateResponse allocateResponse;

		// loop until we get a container to startup in.
		// TODO: need a little util to go fetch containers
		int numCon = 0;
		while (numCon < numContainers && rmRequestID < maxRequestAttempts) {
			LOG.info("resource try #" + rmRequestID);
			req.setResponseId(rmRequestID);
			allocateResponse = resourceManager.allocate(req);
			LOG.info("allocate response sent: " + req.getResponseId());

			// Get AMResponse from AllocateResponse
			AMResponse amResp = allocateResponse.getAMResponse();
			LOG.info("response received: " + amResp.getResponseId());

			// Retrieve list of allocated containers from the response
			List<Container> givenContainers = amResp.getAllocatedContainers();
			LOG.info("containers given to AM: " + givenContainers.size());
			if( givenContainers.size() > 0) {
				allocatedContainers.addAll(givenContainers);
				numCon = numCon + givenContainers.size();
			}
			rmRequestID++;
		}
		return allocatedContainers;
	}
	
	
	/**
	 * Connects to the given containers manager (the nodemanager really I think)
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
	
	//TODO: there is duplicated code with this and YarnClient.submitApplication, refactor
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
		LOG.info("request sent to start container: " + container.getId().toString());
	}
	
}