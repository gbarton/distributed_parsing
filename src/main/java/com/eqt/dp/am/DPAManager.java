package com.eqt.dp.am;

import java.io.IOException;

public class DPAManager extends YarnAppManager {
	
	

	public DPAManager() throws IOException {
		super();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		DPAManager man = new DPAManager();
		System.out.println("sleeping");
		while(true)
			Thread.sleep(1000);
	}

	
}
