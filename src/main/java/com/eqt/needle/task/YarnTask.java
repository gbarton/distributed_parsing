package com.eqt.needle.task;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.eqt.needle.BasicYarnService;
import com.eqt.needle.notification.DiscoveryService.*;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.KafkaUtils;
import com.eqt.needle.notification.StatusReporter;

/**
 * This class is responsible for starting up and connecting to the command 
 * queue for reading/writing from.
 */
public class YarnTask extends BasicYarnService {
	protected static final Log LOG = LogFactory.getLog(YarnTask.class);

	public YarnTask(String serviceName) throws IOException {
		super(serviceName);
		LOG.info("*******************");
		LOG.info("YarnTask comming up");
		LOG.info("*******************");
		
		String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		containerId = ConverterUtils.toContainerId(containerIdString);
		
		//startup communications
		//init basic topics and a producer
		prod = KafkaUtils.getProducer(this.brokerUri);
		statusReporter = new StatusReporter(prod,this.brokerUri,status);
		discoveryService = new DiscoveryService(prod, this.serviceName, this.brokerUri);
		discoveryService.sendInfo(prod, SERVICE.HOST , this.host);
	}

}
