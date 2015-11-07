package com.tibco.mcqueary.jmsperf;

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import javax.naming.NamingException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutiveTest {

	final static Logger logger = Logger.getLogger(ExecutiveTest.class);
	
	private static String[] tokenize(String str)
	{
		if (str == null)
			return null;
		else
			return str.split("\\s+");	
	}
	
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

	public final void testExecutiveCommandLine(String line) throws IllegalArgumentException, ConfigurationException
	{
		System.err.println("ExecutiveTest: jmsperf " + line);
		Executive.main(tokenize(line));
	}
	

	public boolean testProducerIntegerOption(String optName, String propName) throws IllegalArgumentException, ConfigurationException, NamingException
	{
		int randomInt = TestUtils.getRandomInt(1,100);
		String val = String.valueOf(randomInt);
		
		String[] args = { "--producer", "--"+optName,  val };
		System.err.print("Testing ");
		for (String arg : args)
			System.err.print(arg + " ");
		System.err.println();
		
		Executive executive = new Executive(args);
		Configuration config = executive.getConfig();

		return (randomInt == config.getInt(propName));
	}
	
	@Test
	public final void testParseIntegerValues()
	{

		try {
			
			if (!testProducerIntegerOption("connections",Constants.PROP_CLIENT_CONNECTIONS))
				fail(Constants.PROP_CLIENT_CONNECTIONS);
			
			if (!testProducerIntegerOption("sessions",Constants.PROP_CLIENT_SESSIONS))
				fail(Constants.PROP_CLIENT_SESSIONS);
			
			if (!testProducerIntegerOption("duration",Constants.PROP_DURATION_SECONDS))
				fail(Constants.PROP_DURATION_SECONDS);

			if (!this.testProducerIntegerOption("count", Constants.PROP_MESSAGE_COUNT))
					fail(Constants.PROP_MESSAGE_COUNT);

			if (!this.testProducerIntegerOption("report", Constants.PROP_REPORT_INTERVAL_SECONDS))
				fail(Constants.PROP_REPORT_INTERVAL_SECONDS);

			if (!this.testProducerIntegerOption("txnsize", Constants.PROP_TRANSACTION_SIZE))
				fail(Constants.PROP_TRANSACTION_SIZE);

		} catch (IllegalArgumentException | ConfigurationException
				| NamingException e) {
			fail("error parsing integer value");
			e.printStackTrace();
		}
	}
	
	@Test
	public final void testParseBooleanValues()
	{
		try {
			String[] args = { "--producer", "--uniquedests", Boolean.toString(true) };
			Configuration config =  new Executive(args).getConfig();
			if (!config.getBoolean(Constants.PROP_UNIQUE_DESTINATIONS)==true)
				fail(Constants.PROP_UNIQUE_DESTINATIONS);
				
			String[] args2 = { "--producer", "--xa", Boolean.toString(true) };
			config =  new Executive(args2).getConfig();
			if (!config.getBoolean(Constants.PROP_TRANSACTION_XA)==true)
				fail(Constants.PROP_TRANSACTION_XA);
			
			String[] args3 = { "--producer", "--timestamp", Boolean.toString(true) };
			config =  new Executive(args3).getConfig();
			if (!config.getBoolean(Constants.PROP_PRODUCER_TIMESTAMP)==true)
				fail(Constants.PROP_PRODUCER_TIMESTAMP);

			String[] args4 = { "--producer", "--compress", Boolean.toString(true) };
			config =  new Executive(args4).getConfig();
			if (!config.getBoolean(Constants.PROP_PRODUCER_COMPRESSION)==true)
				fail(Constants.PROP_PRODUCER_COMPRESSION);

		} catch (IllegalArgumentException e) {
			fail(e.getMessage());
		}
	}
	
	
	@Test
	public final void testHelp() throws IllegalArgumentException, ConfigurationException, ParseException, NamingException
	{
		String[] args = { "-help" };
		Executive.main(args);
	}
	
	@Test
	public final void testVersion() throws IllegalArgumentException, ConfigurationException, ParseException, NamingException
	{
		String[] args = { "-version" };
		Executive.main(args);
	}

	@Test
	public final void testTopicAndQueue() throws IllegalArgumentException, ConfigurationException, ParseException, NamingException
	{
		String[] args = { "-consumer", "-topic", "foo", "-queue", "bar"};
		Executive.main(args);
	}

	@Test
	public final void testProducerAndConsumer() throws IllegalArgumentException, ConfigurationException, ParseException, NamingException
	{
		String[] args = { "-consumer", "-producer"};
		Executive.main(args);
	}
	
	@Test 
	public final void testExecutive() throws IllegalArgumentException, ConfigurationException, InterruptedException, NamingException {

		int numToProduce, rate, numProducerConnections, numProducerSessions, numConsumerConnections,
		numConsumerSessions, numToConsume;

		boolean uniqueDests=false;
		int numDestinations=1;

		//defaults
		numToProduce=10000;
		numProducerConnections=1;
		numProducerSessions=numProducerConnections;
		rate=0;
		
		if (uniqueDests)
		{
			//Producer settings
			numProducerSessions=numDestinations;
			numConsumerSessions=numDestinations;
		}			
		//AbstractConsumer settings
		numConsumerConnections=100;
		numConsumerSessions=numConsumerConnections;
		numToConsume = numToProduce * numProducerSessions * numConsumerSessions;

		
		String[] consumerArgs=new String[] { "-consumer", 
				"-connections", String.valueOf(numConsumerConnections),
				"-sessions", String.valueOf(numConsumerSessions),
				"-count", String.valueOf(numToConsume),
				"-warmup", "10",
				
				};
		
		String[] producerArgs=new String[] { "-producer", 
				"-connections", String.valueOf(numProducerConnections),
				"-sessions", String.valueOf(numProducerSessions),
				"-count", String.valueOf(numToProduce),
				"-rate", String.valueOf(rate),
				"-timestamp"
				};

		CountDownLatch startSignal=new CountDownLatch(1);
		Thread consumerThread = new JMSWorkerThread(new Executive(consumerArgs), "CONSUMER", startSignal,null);
		consumerThread.start();
				
		Thread producerThread = new JMSWorkerThread(new Executive(producerArgs), "PRODUCER", startSignal, null);
		producerThread.start();
		
		System.err.println("Sleeping 5 seconds");
		Thread.sleep(5000);
		
		System.err.println("Ready...set....GO!!!");		
		startSignal.countDown();
		try {
			consumerThread.join();
			producerThread.join();
		} catch (InterruptedException e) {
			logger.debug("Interrupted: ", e);
		}
		logger.info("testExecutive done.");
		
	}
}
