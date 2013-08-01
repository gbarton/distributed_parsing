package com.eqt.needle.sample;

import java.io.IOException;

import com.eqt.dp.client.YarnClient;

/**
 * Sample client class to show you how to lauch an ApplicationMaster into
 * Yarn.  Not much work involved to get up and running.
 * 
 * The sample yarn application that runs will monitor stats of a given cluster through a brute force
 * method of always pinging the RM for new containers and checking to see if it knows of that host yet.
 * 
 * @author gman
 *
 */
public class StatMonitorExampleClient extends YarnClient {

	public StatMonitorExampleClient(String zkURI) throws IOException {
		super(zkURI);
		this.amClassName = "com.eqt.needle.sample.StatMonitorSampleAM";
		this.pathToHDFSJar = "/dp.jar";
		//default is 512, can override if desired.
		this.amMemory = 512;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length != 1) {
			System.out.println("USAGE: StatMonitorExampleClient <zkUri>");
			System.exit(1);
		}
		
		StatMonitorExampleClient client = new StatMonitorExampleClient(args[0]);
		client.start();

	}

}
