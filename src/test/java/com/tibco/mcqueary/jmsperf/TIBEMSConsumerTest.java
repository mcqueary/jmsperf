package com.tibco.mcqueary.jmsperf;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tibco.mcqueary.jmsperf.Constants.AckMode;
import com.tibco.mcqueary.jmsperf.Consumer.Builder;

public class TIBEMSConsumerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testAcknowledge() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testProcess() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testTIBEMSConsumer() {
		Builder builder = new Builder();
		
		TIBEMSConsumer consumer = new TIBEMSConsumer(new Builder()
			.messages(10)
			.ackMode(AckMode.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE));
				
	}

}
