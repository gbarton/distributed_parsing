package com.eqt.needle.sample;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import javax.management.MBeanServerConnection;

/**
 * Sample class of some kind of task that runs in yarn. This one reports system
 * usage and pops off periodic messages on the message bus.
 * Right now just monitors system load until I move to JDK 1.7.
 *
 */
public class StatMonitorSampleTask {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();

		OperatingSystemMXBean osMBean = ManagementFactory.newPlatformMXBeanProxy(
		mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
		
		
		
		while(true) {
			double cpu = osMBean.getSystemLoadAverage();
			System.out.println("Cpu usage: "+cpu+"%");
			Thread.sleep(1000);
		}

	}

}
