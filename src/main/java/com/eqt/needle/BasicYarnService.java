package com.eqt.needle;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.eclipse.jetty.util.log.Log;

import kafka.javaapi.producer.Producer;

import com.eqt.needle.constants.STATUS;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.StatusReporter;
import com.eqt.needle.topics.control.KillCommand;
import com.gman.util.Constants;
import com.gman.util.YarnUtils;

public class BasicYarnService implements Closeable {

	// Configuration
	protected Configuration conf;
	protected ContainerId containerId;
	protected ApplicationAttemptId appAttemptID;
	
	//communications
	protected String brokerUri = null;
	protected StatusReporter statusReporter = null;
	protected DiscoveryService discoveryService = null;
	protected Producer<String, String> prod = null;
	
	protected KillCommand killThread = null;
	
	//what does one call thyself
	protected String serviceName = System.currentTimeMillis() + "";

	//where are we?
	protected String host;
//	private int port;
	
	protected STATUS status = STATUS.PENDING;
	
	protected Map<String, String> envs = new HashMap<String, String>();

	/**
	 * lets you specify a custom name for the service running, will append -HostName-TimeStamp
	 * to the end of it to uniquify the name.
	 * @param serviceName
	 * @throws IOException
	 */
	public BasicYarnService(String serviceName) throws IOException {
		
		try {
			this.host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new IOException("unable to determine where I live (get my hostname)",e);
		}
		String append = "-" + host + "-" + System.currentTimeMillis();
		
		if(serviceName != null)
			this.serviceName = serviceName + append;
		else
			this.serviceName = append;
		
		envs.putAll(System.getenv());
		YarnUtils.dumpEnvs();
		conf = new YarnConfiguration();
		
		
		//will only work for tasks, but eh, gets it done.
		if(envs.containsKey(Constants.BROKER_URI))
			this.brokerUri = envs.get(Constants.BROKER_URI);
	}
	
	public BasicYarnService() throws IOException {
		this(null);
	}
	
	public String getHost() {
		return host;
	}
	
	public ContainerId getContainerId() {
		return containerId;
	}
	
	/**
	 * MUST always get called.. makes a mess otherwise.
	 * Highly Recommended to override, call this one, then do your 
	 * own cleanup before calling System.exit();
	 */
	public void close() {
		Log.info("going offline");
		if(statusReporter != null)
			statusReporter.close();
		if(discoveryService != null)
			discoveryService.close();
		if(prod != null)
			prod.close();
	}
	
}
