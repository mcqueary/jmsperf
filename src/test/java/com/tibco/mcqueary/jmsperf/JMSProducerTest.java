package com.tibco.mcqueary.jmsperf;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JMSProducerTest {

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
	public final void testSetup() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testJMSProducer() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testToBytes() {
		String test = "4k";
		int expectedValue = 4 * 1024;
		int result = ConfigHandler.toBytes(test);
		assertEquals(result,expectedValue);
	}

}
