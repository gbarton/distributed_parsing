package com.gman.notification;

/**
 * Base Class for notifications traveling through the system.
 * @author gman
 *
 */
public interface EventProcessor {

	/**
	 * Sends a control event, not really used except
	 * for the master to issue command and control over slaves
	 * and for slaves to report broken/done.
	 */
	public void sendControlEvent(WorkUnit work, Control c);
	
	/**
	 * If there is an existing control to process return it
	 * else null.
	 * @return
	 */
	public Control getControlEvent();
	
	public void sendWorkUnitComplete(WorkUnit unit);
	
	public void createdNewWork(WorkUnit from, String path);
	
	public WorkUnit getNextWorkUnit();
	
	public static class WorkUnit {
		public String work;
		public String pathIn = null;
		public String pathOut = null;
		
		public String toString() {
			return "pathIn: " + pathIn + " pathOut: " + pathOut;
		}
	}
	
	public static enum Control {
		START,
		STOP,
		RETRY,
		PAUSE,
		FAILED,
		FINISHED
	}
}
