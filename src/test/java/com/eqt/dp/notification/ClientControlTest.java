package com.eqt.dp.notification;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gman.broker.EmbeddedZK;
import com.gman.broker.StandaloneBroker;
import com.gman.notification.ClientControl;
import com.gman.notification.EventProcessor.Control;
import com.gman.notification.EventProcessor.WorkUnit;
import com.gman.notification.Message;
import com.gman.notification.ServerControl;

public class ClientControlTest {

	private final EmbeddedZK zk = new EmbeddedZK();
	StandaloneBroker broker = null;
	WorkUnit u = new WorkUnit("/pathIn","/pathOut");
	Control c = Control.START;
	ServerControl ser = null;
	ClientControl con = null;

	@Before
	public void setUp() throws Exception {
		Thread tz = new Thread(zk,"zk");
		tz.start();
		broker = new StandaloneBroker(zk.getURI());
		Thread tb = new Thread(broker,"broker");
		tb.start();
		ser = new ServerControl(broker.getURI(), broker.getZKURI());
		con = new ClientControl(broker.getURI(),broker.getZKURI());

	}

	@After
	public void tearDown() throws Exception {
		if(broker != null)
			broker.shutDown();
		if(zk != null)
			zk.shutdown();
	}

	@Test
	public void test() throws UnknownHostException, InterruptedException {
		Message<Control, WorkUnit> m = new Message<Control, WorkUnit>(c, u);
		ser.sendMessage(m);
		
		Message<Control, WorkUnit> message = con.getMessage();
		assertEquals(m, message);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void multiMessageTest() {
		List<Message<Control,WorkUnit>> sendList = new ArrayList<Message<Control,WorkUnit>>();
		List<Message<Control,WorkUnit>> getList = new ArrayList<Message<Control,WorkUnit>>();
		for(int i = 0; i < 5;i++) {
			Message<Control, WorkUnit> m = new Message<Control, WorkUnit>(Control.START,new WorkUnit("in", "out-"+i));
			sendList.add(m);
			ser.sendMessage(m);
		}
		
		Message<Control, WorkUnit> m = con.getMessage();
		
		while(m != null) {
			getList.add(m);
			m = con.getMessage();
		}
		assertEquals(sendList.toArray(), getList.toArray());
	}
	
}
