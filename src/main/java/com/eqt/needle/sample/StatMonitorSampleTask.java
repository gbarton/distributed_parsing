package com.eqt.needle.sample;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import javax.management.MBeanServerConnection;

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
		while(true) {
			double cpu = osMBean.getSystemLoadAverage();
			System.out.println("Cpu usage: "+cpu+"%");
			statusReporter.sendMessage(prod, status, cpu+"");
			Thread.sleep(1000*60);
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
