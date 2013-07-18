package com.eqt.dp.client;

import java.io.IOException;

public class DPClient extends YarnClient {

	public DPClient(String zkURI) throws IOException {
		super(zkURI);
		this.amClassName = "com.eqt.dp.am.DPAManager";
		this.pathToHDFSJar = "/dp.jar";
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length != 1) {
			System.out.println("USAGE: DPClient <zkUri>");
			System.exit(1);
		}
		
		DPClient client = new DPClient(args[0]);
		client.start();

	}

}
