package com.eqt.dp.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
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
import org.apache.zookeeper.data.Stat;

import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;
import com.gman.util.Constants;
import com.gman.util.YarnUtils;

/**
 * Assumes the shaded jar containing the work is loaded into HDFS.
 * Must call constructor and then populate all the important marked vars
 * @author gman
 */
public class YarnClient implements Watcher {
	private static final Log LOG = LogFactory.getLog(YarnClient.class);
	private AtomicInteger init = new AtomicInteger(0);
	
	// Configuration
	protected Configuration conf;
	// YARN RPC to communicate with the Resource Manager or Node Manager
	protected YarnRPC rpc;
	// do the talking to
	protected ClientRMProtocol applicationsManager;
	// AppId
	protected ApplicationId appId;
	//fs, cause its good to have one.
	protected FileSystem fs;
	
	private ZooKeeper zk;
	
	protected String zkURI = null;
	//derived from pathToHDFSJar
	protected String jarName = null;
	//TODO: dynamically go find this one day.
	//REQUIRED
	protected String pathToHDFSJar = null;
	//REQUIRED
	protected int amMemory = 512;
	//REQUIRED
	protected String amClassName = null;
	//REQUIRED, dont have to add anything if you dont wanna.
	protected Map<String,String> otherArgs = new HashMap<String, String>();
	
	protected String brokerUri = null;
	
	protected TopicConsumer clientFeed = null;
	
	public YarnClient(String zkURI) throws IOException {
		
		//TODO: better checking of the uri coming in.
		if (zkURI == null)
			throw new IOException("must pass in a valid zk uri");
		
		conf = new YarnConfiguration();
		LOG.info("fs.default.name set to: " + conf.get("fs.default.name"));
		rpc = YarnRPC.create(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS));
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		Configuration appsManagerServerConf = new Configuration(conf);
		applicationsManager = ((ClientRMProtocol) rpc
				.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
		
		GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse resp = applicationsManager.getNewApplication(request);
		appId = resp.getApplicationId();
		LOG.info("Got new ApplicationId: " + appId);
		LOG.info("Cluster minRam:" + resp.getMinimumResourceCapability().getMemory());
		LOG.info("Cluster maxRam:" + resp.getMaximumResourceCapability().getMemory());

		this.zkURI = zkURI;
		LOG.info("zkconnect string: " + zkURI);
		
		LOG.info("connecting to zookeeper: " + zkURI);
		zk = new ZooKeeper(zkURI,3000,this);
		LOG.info("spinning until initialization complete.");
		while(init.get() < 2)
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				LOG.warn("trouble sleeping.");
			}
		
		LOG.info("completed ZK initialization");

		LOG.info("grabbed a FileSystem handle");
		fs = FileSystem.get(conf);
	}

	//TODO" this is too close to the one in YarnAppManager, refactor??
	@Override
	public void process(WatchedEvent event) {
    	LOG.info("zookeeper connection complete");

    	//no work, release cycle lock #2
		if(init.get() == 1) {
			init.getAndIncrement();
			return;
		}
		
		//TODO: when we go to multiple brokers this has to create more than 1 dir
    	String path = "/" + Constants.ZK_URI_BASE_PATH;
    	try {
    		if(zk.exists(path, false) == null) {
    	    	LOG.info("creating path: " + path);
    			zk.create(path,null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    		}
    		String appPath = path + "/" + appId.toString();
	    	LOG.info("creating path: " + appPath);
	    	zk.create(appPath,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
	    	String propsPath = appPath + Constants.ZK_CLIENT_BASE_PATH;
	    	LOG.info("creating path: " + propsPath);
	    	zk.create(propsPath,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			LOG.info("base store paths created");
			if(!this.zkURI.endsWith("/")) //not final / (i think thats invalid anyways??)
				this.zkURI += "/";
			//add our root.
			this.zkURI += Constants.ZK_URI_BASE_PATH + "/" + appId.toString();

		} catch (KeeperException e) {
			LOG.error("total fail with zk", e);
			System.exit(1);
		} catch (InterruptedException e) {
			LOG.error("total fail with zk", e);
			System.exit(1);
		} finally {
			//we close this con so we can open another one rooted.
			try {
				zk.close();
			} catch (InterruptedException e) {
				//gulp
			} finally {
				try {
			    	//release spin cycle lock #1
			    	init.getAndIncrement();
					zk = new ZooKeeper(this.zkURI,3000,this);
				} catch (IOException e) {
					LOG.error("zk is mis behaving.",e);
					System.exit(1);
				}
			}
		}
	}
	
	/**
	 * Launches the app and goes into a spin cycle until the end of time.
	 * (or until the user hits q)
	 */
	public void start() throws IOException, InterruptedException {
		//lets make a dir in HDFS to use:
		fs.mkdirs(new Path("/"+appId.toString()));
		
		if(pathToHDFSJar == null || pathToHDFSJar.length() == 0) {
			throw new IOException("pathToHDFSJar not present");
		}
		
		Path p = new Path(pathToHDFSJar);
		//not pretty I know.
		int idx = pathToHDFSJar.lastIndexOf("/");
		if (idx == -1)
			idx = 0;
		else
			idx = idx + 1;
		String jarName = pathToHDFSJar.substring(idx);
		LOG.info("Extracted Jar name: " + jarName);
		
		if(amClassName == null || amClassName.length() == 0)
			throw new IOException("amClassName not found");
		
		submitApplication(appId,amClassName, amMemory, jarName, fs.getFileStatus(p), otherArgs);
		
		GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
		reportRequest.setApplicationId(appId);
		GetApplicationReportResponse reportResponse = applicationsManager.getApplicationReport(reportRequest);
		ApplicationReport report = reportResponse.getApplicationReport();
		LOG.info("AppManager up on: " + report.getHost() + ":" + report.getRpcPort());
		LOG.info("diag: " + report.getDiagnostics());
		
		LOG.info("waiting for broker notification");
		String propsPath = Constants.ZK_CLIENT_BASE_PATH;
		//TODO: could use a watch here instead..
		while(true)
			try {
				List<String> children = zk.getChildren(propsPath, false);
				if(children.contains(Constants.CLIENT_BROKER_LOCATION)) {
					this.brokerUri = new String(zk.getData(
							propsPath+"/"+Constants.CLIENT_BROKER_LOCATION,
							false, new Stat()));
					break;
				}
				Thread.sleep(100);
			} catch (InterruptedException e) {
				LOG.warn("trouble sleeping.");
			} catch (KeeperException e) {
				LOG.error("zk connection lost.");
				throw new IOException("zk flaked on us",e);
			}
		LOG.info("broker receieved: " + brokerUri);
		clientFeed = new TopicConsumer(Constants.TOPIC_CLIENT_FEED, this.brokerUri);

		long start = System.currentTimeMillis();
		LOG.info("going into spin cycle:");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			if (br.ready()) {
				String line = br.readLine();
				LOG.info("entered: '" + line + "'");

				if (line == null)
					LOG.info("unknown command");
				else if (line.equals("q")) {
					close();
					break;
				} else if (line.equals("r")) {
					report = reportResponse.getApplicationReport();
					LOG.info("appId: " + appId + " state: " + report.getYarnApplicationState().toString());
					LOG.info("on host: " + report.getHost() + ":" + report.getRpcPort());
				}
			}

			//read messages:
			Message<String,String> message = clientFeed.getNextStringMessage();
			while(message != null) {
				LOG.info("Status reported from Task: "+ message.key + " " + message.value);
				message = clientFeed.getNextStringMessage();
			}
			
			Thread.sleep(500);
			long now = System.currentTimeMillis();
			if (now - start > 5000) {
				start = now;
				// TODO: needs to be allocate call to to status update
				reportResponse.getApplicationReport();
			}
		}

		if (br != null)
			br.close();
		System.exit(0);
		
	}
	
	public void close() throws IOException {
		LOG.info("killing");

		// kill it
		KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);
		killRequest.setApplicationId(appId);
		applicationsManager.forceKillApplication(killRequest);
		
		//TODO: clean the directory up in HDFS
		fs.close();
		
		if(zk != null)
			try {
				zk.close();
			} catch (InterruptedException e) {
				LOG.warn("had issues closing zk conn");
			}
		
		
	}
	
	/**
	 * Hopefully Generic setup of a manager jar.
	 * 
	 * @param appId 	-get from the RM
	 * @param appName	-full className of Master class
	 * @param amMemory	-ram needed
	 * @param jarName	-name of jar containing Manager
	 * @param jarStatus	-status to pull bits off of pointing to the jar
	 * @param args		-args to pass down to the class. TODO: make this work
	 * @throws YarnRemoteException
	 */
	private void submitApplication(ApplicationId appId, String appName, int amMemory, String jarName,
			FileStatus fileStatus, Map<String,String> args) throws YarnRemoteException {
		ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
		appContext.setApplicationId(appId);
		appContext.setApplicationName(appName);

		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
		amJarRsrc.setType(LocalResourceType.FILE);
		amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()));
		LOG.info("resource url " + amJarRsrc.getResource().toString());
		amJarRsrc.setTimestamp(fileStatus.getModificationTime());
		amJarRsrc.setSize(fileStatus.getLen());
		localResources.put(jarName, amJarRsrc);
		amContainer.setLocalResources(localResources);

		Map<String, String> env = new HashMap<String, String>(args);
		// TODO: BS!!! classpath so doesn't work right, am relying on hadoop classpath and a fat jar here.
		String classPathEnv = "$(hadoop classpath):./*:";
		env.put("CLASSPATH", classPathEnv);
		env.put(Constants.ENV_CLASSPATH, classPathEnv);
		env.put(Constants.ENV_JAR_PATH,pathToHDFSJar);
		env.put(Constants.ENV_JAR, jarName);
		env.put(Constants.ENV_NAME,appId.toString());
		env.put(Constants.ENV_ZK_URI,this.zkURI);
		amContainer.setEnvironment(env);

		// Construct the command to be executed on the launched container
//		 String command = "echo CLASSPATH=$CLASSPATH GCP=$(hadoop classpath)"
		String command = "${JAVA_HOME}" + "/bin/java -Xmx" + amMemory + "m " + appName + " 1>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out" + " 2>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err";

		List<String> commands = new ArrayList<String>();
		LOG.info("App launch Command: " + command);
		commands.add(command);
		amContainer.setCommands(commands);

		amContainer.setResource(YarnUtils.getResource(amMemory));
		appContext.setAMContainerSpec(amContainer);

		SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
		appRequest.setApplicationSubmissionContext(appContext);
		applicationsManager.submitApplication(appRequest);
	}
}
