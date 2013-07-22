package com.eqt.needle.sample;

import java.io.IOException;

import com.eqt.dp.am.YarnAppManager;

/**
 * Sample application master using needle's YarnAppManager to do the tough stuffs.
 * @author gman
 */
public class StatMonitorSampleAM extends YarnAppManager {

	public StatMonitorSampleAM() throws IOException {
		super();
	}

	public void start() {
		
		
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
