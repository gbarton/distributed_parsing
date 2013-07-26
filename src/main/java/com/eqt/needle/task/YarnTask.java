package com.eqt.needle.task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.gman.util.YarnUtils;

/**
 * This class is responsible for starting up and connecting to the command 
 * queue for reading/writing from.
 */
public class YarnTask {
	private static final Log LOG = LogFactory.getLog(YarnTask.class);
	private AtomicBoolean init = new AtomicBoolean(false);
	private ContainerId containerId;
	private ApplicationAttemptId appAttemptID;
	// Configuration
	private Configuration conf;
	Map<String, String> envs = new HashMap<String, String>();


	public YarnTask() {
		LOG.info("*******************");
		LOG.info("YarnAppManager comming up");
		LOG.info("*******************");
		envs.putAll(System.getenv());
		YarnUtils.dumpEnvs();
		
		conf = new YarnConfiguration();
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);

	}
	
}
