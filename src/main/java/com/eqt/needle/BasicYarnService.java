package com.eqt.needle;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import kafka.javaapi.producer.Producer;

import com.eqt.needle.constants.STATUS;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.StatusReporter;
import com.gman.util.Constants;
import com.gman.util.YarnUtils;

public class BasicYarnService {

	// Configuration
	protected Configuration conf;
	protected ContainerId containerId;
	protected ApplicationAttemptId appAttemptID;
	
	//communications
	protected String brokerUri = null;
	protected StatusReporter statusReporter = null;
	protected DiscoveryService discoveryService = null;
	protected Producer<String, String> prod = null;
	
	protected STATUS status = STATUS.PENDING;
	
	protected Map<String, String> envs = new HashMap<String, String>();

	
	public BasicYarnService() {
		envs.putAll(System.getenv());
		YarnUtils.dumpEnvs();
		conf = new YarnConfiguration();
		//will only work for tasks, but eh, gets it done.
		if(envs.containsKey(Constants.BROKER_URI))
			this.brokerUri = envs.get(Constants.BROKER_URI);

	}
	
}
