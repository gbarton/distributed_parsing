package com.gman.notification.satus;

import java.io.IOException;

import kafka.producer.KeyedMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.eqt.needle.constants.STATUS;
import com.eqt.needle.notification.Message;
import com.gman.notification.BaseNeedleCom;
import com.gman.notification.ConsumerThread;

public class StatusCom extends BaseNeedleCom<STATUS, String> {
	private static final Log LOG = LogFactory.getLog(StatusCom.class);
	final ObjectMapper mapper = new ObjectMapper();

	public StatusCom(String broker, String zkuri, String topicTo, String topicFrom) {
		super(broker, zkuri, topicTo, topicFrom);

		c = new ConsumerThread<STATUS, String>(this.topicFrom, brokerZKURI) {
			@Override
			public String readValue(byte[] value) {
				try {
					LOG.info("readValue about to decode: " + new String(value));
					return mapper.readValue(new String(value), String.class);
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
			public STATUS readKey(byte[] key) {
				LOG.info("readKey about to decode: " + new String(key));
				return STATUS.valueOf(new String(key));
			}
		};

		executor.submit(c);
		c.init();

	}

	@Override
	public void sendMessage(Message<STATUS, String> message) {
		KeyedMessage<String, String> mess;
		try {
			mess = new KeyedMessage<String, String>(topicTo, message.key.toString(),
					mapper.writeValueAsString(message.value));
			producer.send(mess);
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
