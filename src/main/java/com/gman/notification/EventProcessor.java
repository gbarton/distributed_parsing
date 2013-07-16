package com.gman.notification;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
	
	/**
	 * little interface to build to if you are going to send messages
	 * over Kafka
	 */
	public interface KMesg<T> {
		public String pack();
		public T unpack(String packed);
	}
	
	public static class WorkUnit implements KMesg<WorkUnit> {
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((pathIn == null) ? 0 : pathIn.hashCode());
			result = prime * result + ((pathOut == null) ? 0 : pathOut.hashCode());
			result = prime * result + ((work == null) ? 0 : work.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			WorkUnit other = (WorkUnit) obj;
			if (pathIn == null) {
				if (other.pathIn != null)
					return false;
			} else if (!pathIn.equals(other.pathIn))
				return false;
			if (pathOut == null) {
				if (other.pathOut != null)
					return false;
			} else if (!pathOut.equals(other.pathOut))
				return false;
			if (work == null) {
				if (other.work != null)
					return false;
			} else if (!work.equals(other.work))
				return false;
			return true;
		}

		public String work;
		public String pathIn = null;
		public String pathOut = null;
		
		public WorkUnit() {
			
		}
		
		public WorkUnit(String pathIn, String pathOut) {
			this.pathIn = pathIn;
			this.pathOut = pathOut;
		}
		
		public String toString() {
			return "pathIn: " + pathIn + " pathOut: " + pathOut;
		}

		@Override
		public String pack() {
			ObjectMapper mapper = new ObjectMapper();
			try {
				return mapper.writeValueAsString(this);
			} catch (JsonGenerationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public WorkUnit unpack(String packed) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				return mapper.readValue(packed,WorkUnit.class);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
		
		public void clone(WorkUnit cloneMe) {
			this.pathIn = new String(cloneMe.pathIn);
			this.pathOut = new String(cloneMe.pathOut);
			this.work = new String(cloneMe.work);
		}
	}
	
	public static enum Control implements KMesg<Control> {
		START,
		STOP,
		RETRY,
		PAUSE,
		FAILED,
		FINISHED;

		@Override
		public String pack() {
			return this.toString();
		}

		@Override
		public Control unpack(String packed) {
			return Control.valueOf(packed);
		}
	}
}
