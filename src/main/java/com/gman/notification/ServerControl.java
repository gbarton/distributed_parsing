package com.gman.notification;

import com.gman.util.Constants;

public class ServerControl extends ControlConsumer {

	public ServerControl(String broker, String zkuri) {
		super(broker, zkuri, Constants.TOPIC_CONTROL_DOWN, Constants.TOPIC_CONTROL_UP);
	}
}
