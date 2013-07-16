package com.gman.notification;

import com.gman.util.Constants;

public class ClientControl extends ControlConsumer {
	
	public ClientControl(String broker, String zkuri) {
		super(broker, zkuri, Constants.TOPIC_CONTROL_UP, Constants.TOPIC_CONTROL_DOWN);
	}

}
