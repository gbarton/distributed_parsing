package com.eqt.dp.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.gman.util.Constants;
import com.gman.util.YarnUtils;

/**
 * Assumes the shaded jar containing the work is loaded into HDFS.
 * Must call constructor and then populate all the important marked vars
 * @author gman
 */
public class YarnClient {
	private static final Log LOG = LogFactory.getLog(YarnClient.class);
	
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
	
	
	public YarnClient(String zkURI) throws IOException {
		//TODO: better checking
		if (zkURI == null)
			throw new IOException("must pass in a valid zk uri");
		this.zkURI = zkURI;
		
		conf = new YarnConfiguration();
		LOG.info("fs.default.name set to: " + conf.get("fs.default.name"));
		rpc = YarnRPC.create(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS));
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		Configuration appsManagerServerConf = new Configuration(conf);
		applicationsManager = ((ClientRMProtocol) rpc
				.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
		LOG.info("grabbed a FileSystem handle");
		fs = FileSystem.get(conf);
	}

	/**
	 * Launches the app and goes into a spin cycle until the end of time.
	 * (or until the user hits q)
	 */
	public void start() throws IOException, InterruptedException {
		GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse resp = applicationsManager.getNewApplication(request);
		appId = resp.getApplicationId();
		LOG.info("Got new ApplicationId: " + appId);
		LOG.info("Cluster minRam:" + resp.getMinimumResourceCapability().getMemory());
		LOG.info("Cluster maxRam:" + resp.getMaximumResourceCapability().getMemory());
		
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
		
	}
	
	public void close() throws IOException {
		LOG.info("killing");

		// kill it
		KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);
		killRequest.setApplicationId(appId);
		applicationsManager.forceKillApplication(killRequest);
		
		//TODO: clean the directory up in HDFS
		fs.close();
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
		//TODO:hardcodes bad
		env.put(Constants.ENV_ZK_URI,this.zkURI);
//		env.put(Constants.ENV_ZK_URI, "localhost:2181");
//		env.put(Constants.ENV_HDFS_URI,"hdfs://localhost:8020");
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
