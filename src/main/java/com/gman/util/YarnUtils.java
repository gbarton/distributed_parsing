package com.gman.util;

import java.util.Set;
import java.util.TreeSet;

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
	 * returns a request for it. Ram is bumped up to the next whole unit that the cluster operates at.
	 * So if the cluster has min = 100MB, and you request 150MB, you will get 200MB.
	 * @param min -cluster Min resource
	 * @param max -cluster max resource
	 * @param desiredMemory -amount of ram wanted
	 * @param numContainers -how many containers like this to request
	 * @return
	 */
	public static ResourceRequest getResRequest(Resource min, Resource max, int desiredMemory, int numContainers) {
		LOG.info("resource request " + min + " " + max + " " + desiredMemory + " num: " + numContainers);
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
			ram = new Long(Math.round(Math.ceil(desiredMemory/min.getMemory())*min.getMemory())).intValue(); //i think this will give close enough
			
		rsrcRequest.setCapability(YarnUtils.getResource(ram));
		rsrcRequest.setNumContainers(1);
		LOG.info("resource requested setup with ram: " + rsrcRequest.getCapability().getMemory());
		return rsrcRequest;
	}
	
	public static void dumpEnvs() {
		LOG.info("currently known enviroment variables");
		Set<String> keys = new TreeSet<String>(System.getenv().keySet()); 
		for(String key : keys)
			LOG.info(key + ": " + System.getenv(key));
	}
	
}
