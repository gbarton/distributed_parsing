package com.eqt.needle.notification;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.eqt.needle.constants.STATUS;

public class StatusReporter extends TopicConsumer {

	public static final String TOPIC_TO_AM = "status_to_am";
	public static final String TOPIC_TO_TASKS = "status_to_tasks";
	
	private boolean isAM = false;
	
	/**
	 * Default constructor for a task to use.
	 * @param brokerUri
	 */
	public StatusReporter(Producer<String,String> prod,String brokerUri,STATUS initialStatus) {
		this(prod,brokerUri,initialStatus,false);
	}
	
	/**
	 * call with isAM = true for setting up consumer for AM.
	 * @param brokerUri
	 * @param isAM
	 */
	public StatusReporter(Producer<String,String> prod, String brokerUri,STATUS initialStatus, boolean isAM) {
		super(isAM?TOPIC_TO_TASKS:TOPIC_TO_AM,brokerUri);
		this.isAM = isAM;
	}
	
	public void sendMessage(Producer<String, String> prod, STATUS key, String value) {
		prod.send(new KeyedMessage<String, String>(this.isAM?TOPIC_TO_TASKS:TOPIC_TO_AM,key.toString(),value));
	}
	
	/**
	 * Used to seed the topic with data, since the consumer will gladly blow up trying to attach.
	 * Only has to be called once.
	 * @param prod
	 * @param isAM -is this the AM or not?
	 * @param key
	 * @param value
	 */
	public static void sendMessage(Producer<String, String> prod,boolean isAM) {
		prod.send(new KeyedMessage<String, String>(isAM?TOPIC_TO_TASKS:TOPIC_TO_AM,STATUS.PENDING.toString(),"init"));
	}
	
	
	public Message<STATUS,String> getNextMessage() {
		Message<String,String> stringMessage = getNextStringMessage();
		if(stringMessage != null) {
			return new Message<STATUS, String>(STATUS.valueOf(stringMessage.key), stringMessage.value);
		} else
			return null;
	}
}
