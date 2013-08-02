package com.eqt.needle.sample;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import javax.management.MBeanServerConnection;

import org.codehaus.jackson.map.ObjectMapper;

import com.eqt.needle.constants.STATUS;
import com.eqt.needle.task.YarnTask;

/**
 * Sample class of some kind of task that runs in yarn. This one reports system
 * usage and pops off periodic messages on the message bus.
 * Right now just monitors system load until I move to JDK 1.7.
 *
 */
public class StatMonitorSampleTask extends YarnTask {

	public StatMonitorSampleTask() throws IOException, InterruptedException {
		super("StatMonitor");
		MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();

		OperatingSystemMXBean osMBean = ManagementFactory.newPlatformMXBeanProxy(
		mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
		status = STATUS.STARTED;
		statusReporter.sendMessage(prod, status, "");
		ObjectMapper mapper = new ObjectMapper();
		while(true) {
			double cpu = osMBean.getSystemLoadAverage();
			Payload p = new Payload();
			p.cores = osMBean.getAvailableProcessors();
			p.loadAverage = osMBean.getSystemLoadAverage();
			p.happenedAt = System.currentTimeMillis() % (10*1000); //nearest 10 seconds
			LOG.info("Status Report: " + mapper.writeValueAsString(p));
			statusReporter.sendMessage(prod, status, mapper.writeValueAsString(p));
			Thread.sleep(1000*5);
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		@SuppressWarnings("unused")
		StatMonitorSampleTask task = new StatMonitorSampleTask();
	}

}
