package com.gman.notification.satus;

public class StatusAM extends StatusCom {

	public StatusAM(String broker, String zkuri) {
		super(broker, zkuri, "statusTo", "statusFrom");
	}
}