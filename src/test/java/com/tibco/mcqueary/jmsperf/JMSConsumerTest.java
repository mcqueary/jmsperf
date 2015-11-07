package com.tibco.mcqueary.jmsperf;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.tibco.mcqueary.jmsperf.Constants.*;
import static com.tibco.mcqueary.jmsperf.Constants.DestType.*;

public class JMSConsumerTest {

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
	public final void testJMSConsumerBuilderConstructor() {
		
		Consumer consumer = new Consumer.Builder()
				.connections(10)
				.sessions(10)
				.duration(300)
				.ackMode(AckMode.CLIENT_ACKNOWLEDGE)
				.brokerURL("tcp://localhost:7222")
				.connectionFactory("GenericConnectionFactory")
				.username("tooey")
				.password("tooey")
				.destType(TOPIC)
				.destination("mytopic")
				.messages(20000)
				.warmup(5)
				.reportInterval(5)
				.build();

	}

	@Test
	public final void testBuilder() {
		fail("Not yet implemented"); // TODO
	}

}
