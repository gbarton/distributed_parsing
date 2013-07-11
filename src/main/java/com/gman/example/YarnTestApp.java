package com.gman.example;

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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetUtils;
//import org.apache.hadoop.security.SecurityInfo;
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
import org.apache.hadoop.yarn.api.records.Resource;
//import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
//import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
//import org.apache.log4j.Logger;

import com.gman.util.Constants;

/**
 * Just gonna see what happens with playing on yarn. NOTE: alot of this is
 * copied from examples, websites. I dont comment this much!
 * 
 * @author gman
 * 
 */
public class YarnTestApp {
	private static final Log LOG = LogFactory.getLog(YarnTestApp.class);
	// ApplicationMaster

	// Configuration
	private Configuration conf;
	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;

	// do the talking to
	private ClientRMProtocol applicationsManager;

	// AppId
	private ApplicationId appId;
	// initial response containing Yarn info
	GetNewApplicationResponse resp;
	FileSystem fs;

	public YarnTestApp() {
		conf = new YarnConfiguration();
		// TODO:hardcode
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		rpc = YarnRPC.create(conf);
	}

	public void init() throws IOException {
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS));
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		Configuration appsManagerServerConf = new Configuration(conf);
		// TODO: do i need this? example code doesnt work
		// appsManagerServerConf.setClass(YarnConfiguration.YARN_SYARN_SECURITY_INFO,
		// ClientRMSecurityInfo.class, SecurityInfo.class);
		applicationsManager = ((ClientRMProtocol) rpc
				.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
		fs = FileSystem.get(conf);
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
	private void createContainer(ApplicationId appId, String appName, int amMemory, String jarName,
			FileStatus fileStatus, String[] args) throws YarnRemoteException {
		// Create a new ApplicationSubmissionContext
		ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
		// set the ApplicationId
		appContext.setApplicationId(appId);
		// set the application name
		appContext.setApplicationName(appName);

		// Create a new container launch context for the AM's container
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		// Define the local resources required
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		// Lets assume the jar we need for our ApplicationMaster is available in
		// HDFS at a certain known path to us and we want to make it available
		// to the ApplicationMaster in the launched container
		LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
		// Set the type of resource - file or archive
		// archives are untarred at the destination by the framework
		amJarRsrc.setType(LocalResourceType.FILE);
		// Set visibility of the resource
		// Setting to most private option i.e. this file will only
		// be visible to this instance of the running application
		amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		// Set the location of resource to be copied over into the
		// working directory
		amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()));
		LOG.info("resource url " + amJarRsrc.getResource().toString());
		// Set timestamp and length of file so that the framework
		// can do basic sanity checks for the local resource
		// after it has been copied over to ensure it is the same
		// resource the client intended to use with the application
		amJarRsrc.setTimestamp(fileStatus.getModificationTime());
		amJarRsrc.setSize(fileStatus.getLen());
		// The framework will create a symlink called AppMaster.jar in the
		// working directory that will be linked back to the actual file.
		// The ApplicationMaster, if needs to reference the jar file, would
		// need to use the symlink filename.
		localResources.put(jarName, amJarRsrc);
		// Set the local resources into the launch context
		amContainer.setLocalResources(localResources);

		// Set up the environment needed for the launch context
		Map<String, String> env = new HashMap<String, String>();
		// For example, we could setup the classpath needed.
		// Assuming our classes or jars are available as local resources in the
		// working directory from which the command will be run, we need to
		// append "." to the path.
		// By default, all the hadoop specific classpaths will already be
		// available in $CLASSPATH, so we should be careful not to overwrite it.
		// TODO: BS!!! classpath so doesnt work right, am relying on hadoop classpath here.
		String classPathEnv = "$(hadoop classpath):./*:";
		env.put("CLASSPATH", classPathEnv);
		env.put(Constants.ENV_CLASSPATH, classPathEnv);
		env.put(Constants.ENV_JAR, jarName);
		env.put(Constants.ENV_NAME,appId.toString());
		//TODO:hardcodes bad
		env.put(Constants.ENV_ZK_URI, "localhost:2181");
		env.put(Constants.ENV_HDFS_URI,"hdfs://localhost:8020");
		amContainer.setEnvironment(env);

		// Construct the command to be executed on the launched container
//		 String command = "echo CLASSPATH=$CLASSPATH GCP=$(hadoop classpath)"
		String command = "${JAVA_HOME}" + "/bin/java -Xmx" + amMemory + "m " + appName + " 1>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out" + " 2>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err";

		List<String> commands = new ArrayList<String>();
		commands.add(command);
		// add additional commands if needed

		// Set the command array into the container spec
		amContainer.setCommands(commands);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(amMemory);
		amContainer.setResource(capability);

		// Set the container launch content into the
		// ApplicationSubmissionContext
		appContext.setAMContainerSpec(amContainer);

		// Create the request to send to the ApplicationsManager
		SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
		appRequest.setApplicationSubmissionContext(appContext);

		// Submit the application to the ApplicationsManager
		// Ignore the response as either a valid response object is returned on
		// success or an exception thrown to denote the failure
		applicationsManager.submitApplication(appRequest);
	}

	public void run() throws IOException, InterruptedException {
		GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
		resp = applicationsManager.getNewApplication(request);
		appId = resp.getApplicationId();
		LOG.info("Got new ApplicationId=" + appId);
		LOG.info("minRam:" + resp.getMinimumResourceCapability().getMemory());
		LOG.info("maxRam:" + resp.getMaximumResourceCapability().getMemory());

		//lets make a dir in HDFS to use:
		fs.mkdirs(new Path("/"+appId.toString()));
		
		final String fileName = "dp.jar";

		Path p = new Path("/"+fileName);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(p, true);
		while (files.hasNext()) {
			LocatedFileStatus fileStatus = files.next();
			LOG.info("path matched file: " + fileStatus.getPath().toString());
		}


		LOG.info("submitting app");
		createContainer(appId, "com.gman.example.SampleAM", 
				resp.getMinimumResourceCapability().getMemory(),
				fileName, fs.getFileStatus(p), null);

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
				else if (line.equals("q"))
					break;
				else if (line.equals("r")) {
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
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		YarnTestApp app = new YarnTestApp();

		app.init();
		app.run();
		app.close();

	}

}
