package com.eqt.needle.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.eqt.dp.am.YarnAppManager;
import com.eqt.needle.constants.STATUS;
import com.eqt.needle.notification.Message;

/**
 * Sample application master using needle's YarnAppManager to do the tough stuffs.
 * @author gman
 */
public class StatMonitorSampleAM extends YarnAppManager {

	private Set<String> hosts = new HashSet<String>();
	
	public StatMonitorSampleAM() throws IOException {
		super();
	}

	public void start() {
		//run forever
		while(true) {
		try {
			List<Container> newContainers = newContainers(1, 512, 10);
			List<ContainerId> releaseContainers = new ArrayList<ContainerId>();
			if(newContainers.size() > 0) {
				for(Container c : newContainers) {
					LOG.info("found new container: " + c.toString());
					//new host, lets add it.
					if(!hosts.contains((c.getNodeHttpAddress()))) {
						LOG.info("launching: " + c.toString());
						hosts.add(c.getNodeHttpAddress());
						launchContainer(c, "com.eqt.needle.sample.StatMonitorSampleTask", 512, null);
						LOG.info("launched: " + c.toString());
					} else 
						releaseContainers.add(c.getId());
				}
				//send back the ones we didnt want.
				releaseContainers(releaseContainers);
			}
			
			Message<STATUS,String> message = statusReporter.getNextMessage();
			while(message != null) {
				LOG.info("Status reported from Task: "+ message.key + " " + message.value);
			}
			Thread.sleep(1000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		StatMonitorSampleAM am = new StatMonitorSampleAM();
		am.start();

		System.out.println("sleeping");
		while(true)
			Thread.sleep(1000);

	}

}
