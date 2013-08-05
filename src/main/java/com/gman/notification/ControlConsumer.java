package com.gman.notification;

import java.io.IOException;

import kafka.producer.KeyedMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.gman.notification.EventProcessor.WorkUnit;

public class ControlConsumer extends BaseNeedleCom<Control,WorkUnit> {
	private static final Log LOG = LogFactory.getLog(ControlConsumer.class);
	
	public ControlConsumer(String broker, String zkuri, String topicTo, final String topicFrom) {
		super(broker,zkuri,topicTo,topicFrom);

		c = new ConsumerThread<Control, WorkUnit>(this.topicFrom,brokerZKURI) {
			ObjectMapper mapper = new ObjectMapper();
			@Override
			public WorkUnit readValue(byte[] value) {
				try {
					LOG.info("readValue about to decode: " + new String(value));
					return mapper.readValue(new String(value),WorkUnit.class);
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
			
			@Override
			public Control readKey(byte[] key) {
				try {
					LOG.info("readKey about to decode: " + new String(key));
					return mapper.readValue(new String(key),Control.class);
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
		}; 
		
		executor.submit(c);
		c.init();
	}
	
	public void sendMessage(Message<Control,WorkUnit> message) {
		sendMessage(message.key, message.value);
	}
	
	private void sendMessage(Control c,WorkUnit work) {
		ObjectMapper mapper = new ObjectMapper();
		LOG.info("@@@@@@@@@@@@");
		LOG.info("topic: " + topicTo + " controlEvent: " + c + " unit: " + work);
		LOG.info("@@@@@@@@@@@@");
		KeyedMessage<String, String> mess;
		try {
			mess = new KeyedMessage<String, String>(topicTo,
							mapper.writeValueAsString(c), mapper.writeValueAsString(work));
			LOG.info("@@@@@@@@@@@@");
			producer.send(mess);
			LOG.info("@@@@@@@@@@@@");
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
	}
}