package com.eqt.needle.topics.control;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.eqt.needle.BasicYarnService;
import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;

/**
 * This implements the kill command on all pieces of the system anyone can send
 * a kill command, AM will receive it, begin the shutdown processes of killing
 * children before it kills itself.
 * 
 * @author gman
 */
public class KillCommand extends TopicConsumer {

	private String sendTopic = null;
	private Producer<String, String> prod = null;
	private WHOAMI whoami = null;

	public static enum WHOAMI {
		CLIENT, AM, TASK
	}

	private static final String CONTROL_CLIENT_TO_AM = "control_client_to_am";
	// private static final String CONTROL_AM_TO_CLIENT =
	// "control_am_to_client";
	private static final String CONTROL_AM_TO_TASK = "control_am_to_task";
	private static final String CONTROL_TASK_TO_AM = "control_task_to_am";

	public KillCommand(WHOAMI who, String brokers, final Producer<String, String> prod, final BasicYarnService closeMe) {
		super(getTopic(who), brokers);

		this.whoami = who;
		this.prod = prod;

		switch (who) {
		case CLIENT:
			this.sendTopic = CONTROL_CLIENT_TO_AM;
			break;
		case AM:
			this.sendTopic = CONTROL_AM_TO_TASK;
			break;
		case TASK:
			this.sendTopic = CONTROL_TASK_TO_AM;
			break;
		default:
			throw new IllegalArgumentException("somehow you gave me a WHOAMI I dont understand");
		}

		Runnable run = null;

		// init the listening thread if its a task
		if (who == WHOAMI.TASK) {
			run = new Runnable() {
				@Override
				public void run() {
					while (true) {
						Message<Control, String> message = getNextMessage();
						if (message != null && message.key == Control.STOP) {
							String id = null;
							if(closeMe.getContainerId() != null)
								id = closeMe.getContainerId().toString();
							else
								id = closeMe.getHost();	
							prod.send(new KeyedMessage<String, String>(sendTopic, Control.STOP.toString(), id));
							closeMe.close();
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOG.warn("cant sleep, too noisy");
						}
					}
				}

			};

		} else if(who == WHOAMI.AM) {
			run = new Runnable() {
				@Override
				public void run() {
					boolean triggered = false;
					long timer = 0; 
					while(true) {
						//theoretically the first message we ever see will be a kill.
						Message<Control,String> message = getNextMessage();
						if(message != null) {
							if(!triggered) {
								WHOAMI whoKilledUs = WHOAMI.valueOf(message.value);
								LOG.info("AM going down because signal from: " + whoKilledUs.toString());
								prod.send(new KeyedMessage<String, String>(sendTopic, Control.STOP.toString(),"now"));
								triggered = true;
								timer = System.currentTimeMillis();
							}
							
						} else {
							if(System.currentTimeMillis() - timer > 10000) {
								LOG.info("timeout triggered. calling close");
								closeMe.close();
							}
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								LOG.warn("cant sleep, too noisy");
							}
						}
					}
				}
			};
		}
		
		if(run != null) {
			Thread t = new Thread(run,"killCommandThread");
			t.start();
		}
	}

	protected Message<Control, String> getNextMessage() {
		Message<String, String> nextStringMessage = getNextStringMessage();
		if (nextStringMessage != null)
			return new Message<Control, String>(Control.valueOf(nextStringMessage.key), nextStringMessage.value);
		return null;
	}

	/**
	 * Initiates a kill command.
	 */
	public void kill() {
		prod.send(new KeyedMessage<String, String>(sendTopic, Control.STOP.toString(), this.whoami.toString()));
	}

	/**
	 * get topic to listen to. clients and tasks listen to the same topic
	 * 
	 * @param who
	 * @return
	 */
	private static String getTopic(WHOAMI who) {
		switch (who) {
		case CLIENT:
			return CONTROL_AM_TO_TASK;
		case AM:
			return CONTROL_TASK_TO_AM;
		case TASK:
			return CONTROL_AM_TO_TASK;
		default:
			throw new IllegalArgumentException("somehow you gave me a WHOAMI I dont understand");
		}

	}

}
