package com.gman.notification;

import com.eqt.needle.notification.Control;

/**
 * Base class for behavior of the Event processor framework.  Forces
 * sub classes to implement runnable so that they can be run in their
 * own thread.
 */
public abstract class BaseProcessor implements EventProcessor, Runnable {

	@Override
	public void sendWorkUnitComplete(WorkUnit unit) {
		sendControlEvent(unit, Control.FINISH);
	}

}
