package com.gman.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

public class YarnUtils {

	private static final Log LOG = LogFactory.getLog(YarnUtils.class);

	public static Resource getResource(int memory) {
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(memory);
		return capability;
	}
	
	/**
	 * takes in the min/max resource of the system and the desired amount of memory and 
	 * returns a request for it.
	 * @param min -cluster Min resource
	 * @param max -cluster max resource
	 * @param numContainers -how many containers like this to request
	 * @return
	 */
	public static ResourceRequest getResRequest(Resource min, Resource max, int desiredMemory, int numContainers) {
		ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);
		rsrcRequest.setHostName("*");
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(1);
		rsrcRequest.setPriority(pri);
		//ram check
		int ram = min.getMemory();
		if(desiredMemory > max.getMemory())
			ram = max.getMemory();
		else
			ram = (desiredMemory/min.getMemory())*min.getMemory(); //i think this will give close enough
			
		rsrcRequest.setCapability(YarnUtils.getResource(ram));
		rsrcRequest.setNumContainers(1);
//		LOG.info("resource requested with ram: " + rsrcRequest.getCapability().getMemory());
		return rsrcRequest;
	}
	
}
