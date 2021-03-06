package com.eqt.dp.am;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.producer.KeyedMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import com.eqt.needle.BasicYarnService;
import com.eqt.needle.broker.StandaloneBroker;
import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.KafkaUtils;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.StatusReporter;
import com.eqt.needle.topics.control.AMControlTopic;
import com.eqt.needle.topics.control.AMTaskControlTopic;
import com.gman.util.Constants;
import com.gman.util.YarnUtils;

public class YarnAppManager extends BasicYarnService implements Watcher {
	protected static final Log LOG = LogFactory.getLog(YarnAppManager.class);
	private AtomicBoolean init = new AtomicBoolean(false);
	
	//override to make the brokers external to the AM, this also will make them
	//redundant and all that stuffs.
	//TODO: make that work :)
	protected boolean embeddedBroker = true;
	private StandaloneBroker broker = null;

	private ZooKeeper zk;

	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;
	private AMRMProtocol resourceManager;
	
	//fs, cause its good to have one.
	protected FileSystem fs;
	
	// cluster stats
	private Resource min;
	private Resource max;
	private int clusterNodes = 1; //yea made big assumption here
	
	private float progress = 0.0f;
	
	protected AMControlTopic controlSignal = null;
	protected AMTaskControlTopic controlTaskSignal = null;
	
	//the containers currently being run by this am.
	private Set<ContainerId> knownContainers = new HashSet<ContainerId>();
	
	public YarnAppManager() throws IOException {
		super("APPMANAGER");
		LOG.info("*******************");
		LOG.info("YarnAppManager comming up");
		LOG.info("*******************");

//		conf.set("fs.defaultFS", envs.get(Constants.ENV_HDFS_URI));
		rpc = YarnRPC.create(conf);
		fs = FileSystem.get(conf);
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		if (containerIdString == null) {
			// container id should always be set in the env by the framework
			throw new IllegalArgumentException("ContainerId not set in the environment");
		}

		containerId = ConverterUtils.toContainerId(containerIdString);
		appAttemptID = containerId.getApplicationAttemptId();
		
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
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);

		RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
		appMasterRequest.setApplicationAttemptId(appAttemptID);
		appMasterRequest.setHost(host);
		//TODO: not sure how to use this yet....
//		appMasterRequest.setTrackingUrl("thisisatest");

		if(embeddedBroker) {
			LOG.info("setting up enbedded broker.");
			broker = new StandaloneBroker(envs.get(Constants.BROKER_ZK_URI));
			Thread tb = new Thread(broker,"broker");
			tb.start();
			envs.put(Constants.BROKER_URI, broker.getURI());
			LOG.info("broker online at: " + broker.getURI());
			this.brokerUri = broker.getURI();
			try {
				zk.create(Constants.ZK_CLIENT_BASE_PATH + "/" + Constants.CLIENT_BROKER_LOCATION, this.brokerUri.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				LOG.error("issues writing broker out to ZK",e);
				throw new IOException("issues writing broker out to ZK",e);
			} catch (InterruptedException e) {
				LOG.error("issues writing broker out to ZK",e);
				throw new IOException("issues writing broker out to ZK",e);
			}
		} else { //using external kafka
			this.brokerUri = envs.get(Constants.BROKER_URI);
		}
		
		//init basic topics and a producer
		prod = KafkaUtils.getProducer(this.brokerUri);
		statusReporter = new StatusReporter(prod,this.brokerUri,status,true);
		discoveryService = new DiscoveryService(prod, "AM", this.brokerUri);
		controlSignal = new AMControlTopic(this.brokerUri);
		controlTaskSignal = new AMTaskControlTopic(this.brokerUri);
		
		LOG.info("registring");
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterRequest);
		min = response.getMinimumResourceCapability();
		max = response.getMaximumResourceCapability();
		LOG.info("registered");
	}

	protected void incProgress(float by) {
		progress+= by;
		if(progress > 1.0f)
			progress = 1.0f;
		
		//TODO: hacky?? how else does one send progress updates/heartbeats to rm?
		try {
			releaseContainers(null);
		} catch (YarnRemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		//TODO: when we go to multiple brokers this has to create more than 1 dir
    	String path = "/kafka"; // + envs.get(Constants.ENV_NAME);
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
			//we need this thing still
//			try {
//				zk.close();
//			} catch (InterruptedException e) {
//				//gulp
//			}
		}
    	//go ahead and add to the constants where to bind the brokers to
    	envs.put(Constants.BROKER_ZK_URI, envs.get(Constants.ENV_ZK_URI)+path);
    	LOG.info("Kafka zk set to: " + envs.get(Constants.BROKER_ZK_URI));
    	init.set(true);
	}
	
	/**
	 * sends a message to the client.
	 * @param update
	 */
	protected void updateClient(String update) {
		updateClient(update,"NO_MESSAGE");
	}

	protected void updateClient(String update, String message) {
		prod.send(new KeyedMessage<String, String>(Constants.TOPIC_CLIENT_FEED, update,message));
	}

	public void close() {
		//first broadcast shut down to the tasks
//		controlTaskSignal.sendControl(prod, Control.STOP, "now");
		
//		//listen for acks on the AMTaskControlTopic
//		while(knownContainers.size() > 0) {
//			Message<String,String> message = controlTaskSignal.getNextStringMessage();
//			if(message != null) {
//				
//			}
//		}
		
//		for(ContainerId c : knownContainers) {
//			//send a message to them individually
//			
//			
//		}
		
		
		try {
			releaseContainers(new ArrayList<ContainerId>(knownContainers));
		} catch (YarnRemoteException e) {
			LOG.warn("difficulties releasing containers",e);
		}
		
		super.close();
		if(controlSignal != null)
			controlSignal.close();

		if(this.broker != null)
			this.broker.shutDown();
	}
	
	/**
	 * Will be inaccurate until a request for resources via newContainer has
	 * been called.
	 * @return
	 */
	protected int getClusterNodes() {
		return clusterNodes;
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
	
	/**
	 * Returns issued containers that the AM has no more need for back to the RM.
	 * @param containers
	 * @throws YarnRemoteException 
	 */
	public void releaseContainers(List<ContainerId> containers) throws YarnRemoteException {
		//keep known containers up to date
		knownContainers.removeAll(containers);

		AllocateRequest req = Records.newRecord(AllocateRequest.class);
		req.setApplicationAttemptId(appAttemptID);
		req.addAllReleases(containers);
		req.setProgress(progress);
		req.setResponseId(0);
		resourceManager.allocate(req);
	}
	
	/**
	 * method for acquiring containers to put to work. Cannot do data locality yet, but will honor
	 * memory requirements. Method may loop a fair few times up to maxRequestAttempts times to try
	 * and get as many of the containers requested as possible before returning the ones given.
	 * @param numContainers
	 * @param memory
	 * @param maxRequestAttempts
	 * @return
	 * @throws IOException
	 */
	public List<Container> newContainers(int numContainers, int memory, int maxRequestAttempts) throws IOException {
		
		List<Container> allocatedContainers = new ArrayList<Container>();
		
		AllocateRequest req = Records.newRecord(AllocateRequest.class);
		// Resource Request
		// Add the list of containers being asked for
		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
		requestedContainers.add(YarnUtils.getResRequest(min, max, memory, numContainers));
		req.addAllAsks(requestedContainers);

		//request ID AND number of attempts all in 1! so nice.
		int rmRequestID = 1;

		// Set ApplicationAttemptId
		req.setApplicationAttemptId(appAttemptID);
		//none to release in this case.
		List<ContainerId> releasedContainers = new ArrayList<ContainerId>();
		req.addAllReleases(releasedContainers);
		req.setProgress(progress);
		
		AllocateResponse allocateResponse;

		// loop until we get a container to startup in.
		// TODO: need a little util to go fetch containers
		int numCon = 0;
		while (numCon < numContainers && rmRequestID <= maxRequestAttempts) {
			req.setResponseId(rmRequestID);
			allocateResponse = resourceManager.allocate(req);
			//TODO: maybe a better way to ask this ahead of time?
			this.clusterNodes = allocateResponse.getNumClusterNodes();
			LOG.info("resource try #" + rmRequestID + " allocate responseID sent: " + req.getResponseId());

			//we need to give the RM a chance to actually put a container together.
			//TODO: theres gotta be a better way!!
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				//gulp
				LOG.warn("troubles sleeping again");
			}
			
			// Get AMResponse from AllocateResponse
			AMResponse amResp = allocateResponse.getAMResponse();

			// Retrieve list of allocated containers from the response
			List<Container> givenContainers = amResp.getAllocatedContainers();
			LOG.info("responseID received: " + amResp.getResponseId() + " containers given to AM: " + givenContainers.size());
			if( givenContainers.size() > 0) {
				allocatedContainers.addAll(givenContainers);
				numCon = numCon + givenContainers.size();
			}
			rmRequestID++;
		}
		
		//keep known containers up to date
		for(Container c : allocatedContainers) 
			knownContainers.add(c.getId());
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
	
	/**
	 * This creates and launches a container
	 * TODO: there is duplicated code with this and YarnClient.submitApplication, refactor
	 * @param container
	 * @param appName
	 * @param amMemory
	 * @param jarName
	 * @param fileStatus
	 * @param args TODO: ignored atm.
	 * @throws IOException 
	 */
	public void launchContainer(Container container, String appName, int amMemory, String[] args) throws IOException {
		ContainerManager cm = getCM(container, conf);
		FileStatus fileStatus = fs.getFileStatus(new Path(envs.get(Constants.ENV_JAR_PATH)));

		// Now we setup a ContainerLaunchContext
		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setContainerId(container.getId());
		ctx.setResource(container.getResource());

		try {
			ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
		} catch (IOException e) {
			LOG.error("Getting current user failed when trying to launch the container", e);
		}

		// Set the environment, will pull along anything setup from the client as well as new broker stuffs.
		Map<String, String> env = new HashMap<String, String>();
		Constants.fill(this.envs, env);
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

		localResources.put(envs.get(Constants.ENV_JAR), shellRsrc);

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
