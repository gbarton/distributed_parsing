package com.eqt.needle.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import com.eqt.needle.BasicYarnService;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.KafkaUtils;
import com.eqt.needle.notification.StatusReporter;

/**
 * This class is responsible for starting up and connecting to the command 
 * queue for reading/writing from.
 */
public class YarnTask extends BasicYarnService {
	private static final Log LOG = LogFactory.getLog(YarnTask.class);

	public YarnTask(String serviceName) {
		super();
		LOG.info("*******************");
		LOG.info("YarnTask comming up");
		LOG.info("*******************");
		
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		
		//startup communications
		//init basic topics and a producer
		prod = KafkaUtils.getProducer(this.brokerUri);
		statusReporter = new StatusReporter(prod,this.brokerUri,status);
		discoveryService = new DiscoveryService(prod, serviceName + "-" + containerIdString, this.brokerUri);
	}
	
}
