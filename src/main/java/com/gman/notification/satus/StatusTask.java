package com.gman.notification.satus;

public class StatusTask extends StatusCom {

	public StatusTask(String broker, String zkuri) {
		super(broker, zkuri, "statusFrom", "statusTo");
	}
}