/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.tibco.mcqueary.jmsperf.ConfigHandler.*;

/**
 * @author Larry McQueary
 *
 */
public class ConfigHandlerTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#instance(java.lang.String)}.
	 */
	@Test
	public final void testInstance() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getProjectConfiguration()}.
	 */
	@Test
	public final void testGetProjectConfiguration() {
		PropertiesConfiguration projectConfig = ConfigHandler.getProjectConfiguration();
		assertNotNull(projectConfig);
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getPropertyNames()}.
	 */
	@Test
	public final void testGetPropertyNames() {
		ConfigHandler handler = ConfigHandler.instance(KEY_EXECUTIVE);
		ArrayList<String> propertyNames = handler.getPropertyNames();
		assertNotNull(propertyNames);
		
		handler = ConfigHandler.instance(KEY_PRODUCER);
		propertyNames = handler.getPropertyNames();
		assertNotNull(propertyNames);

		handler = ConfigHandler.instance(KEY_CONSUMER);
		propertyNames = handler.getPropertyNames();
		assertNotNull(propertyNames);
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getPropertyDescription(java.lang.String)}.
	 */
	@Test
	public final void testGetPropertyDescription() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getValidValues(java.lang.String)}.
	 */
	@Test
	public final void testGetValidValues() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getInstanceConfig()}.
	 */
	@Test
	public final void testGetInstanceConfig() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#updateInstanceConfig(org.apache.commons.configuration.Configuration)}.
	 */
	@Test
	public final void testGetInstanceConfigConfiguration() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#updateInstanceConfig(org.apache.commons.cli.CommandLine)}.
	 */
	@Test
	public final void testGetInstanceConfigCommandLine() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getOptions()}.
	 */
	@Test
	public final void testGetOptions() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#printOptions(java.lang.String, org.apache.commons.cli.Options)}.
	 */
	@Test
	public final void testPrintOptions() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getPropertyName(org.apache.commons.cli.Option)}.
	 */
	@Test
	public final void testGetPropertyName() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getHelpIndex(org.apache.commons.cli.Option)}.
	 */
	@Test
	public final void testGetHelpIndex() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getOptionName(java.lang.String)}.
	 */
	@Test
	public final void testGetOptionName() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getPropertyKeys(java.lang.String)}.
	 */
	@Test
	public final void testGetPropertyKeys() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#listConfig(java.lang.String, org.apache.commons.configuration.Configuration)}.
	 */
	@Test
	public final void testListConfig() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#getConfigString(java.lang.String, org.apache.commons.configuration.Configuration)}.
	 */
	@Test
	public final void testGetConfigString() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#compare(org.apache.commons.cli.Option, org.apache.commons.cli.Option)}.
	 */
	@Test
	public final void testCompare() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#printNames(java.io.PrintStream, java.util.List)}.
	 */
	@Test
	public final void testPrintNames() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#parseClientConfiguration(java.util.Vector)}.
	 * @throws ParseException 
	 */
	@Test
	public final void testParseGoodClientConfiguration() throws ParseException {
		ConfigHandler handler = ConfigHandler.instance(KEY_CONSUMER);
		
		int numConns = TestUtils.getRandomInt(1,100);
		int numSess = TestUtils.getRandomInt(1,100);
		int reportInterval = TestUtils.getRandomInt(1,100);
		int reportWarmup = TestUtils.getRandomInt(1,100);
		
		String[] args = {
			"-consumer",
			"-connections", String.valueOf(numConns),
			"-sessions", String.valueOf(numSess),
			"-report", String.valueOf(reportInterval),
			"-warmup", String.valueOf(reportWarmup),
			"-topic", "foo",
			"-uniquedests"
		};
		PropertiesConfiguration consumerConfig = handler.parseClientConfiguration(args);
		assertNotNull(consumerConfig);
		assertEquals(numConns, consumerConfig.getInt(Constants.PROP_CLIENT_CONNECTIONS));
		assertEquals(numSess, consumerConfig.getInt(Constants.PROP_CLIENT_SESSIONS));
		assertEquals(reportInterval, consumerConfig.getInt(Constants.PROP_REPORT_INTERVAL_SECONDS));
		assertEquals(reportWarmup, consumerConfig.getInt(Constants.PROP_REPORT_WARMUP_SECONDS));
		assertEquals("topic", consumerConfig.getString(Constants.PROP_DESTINATION_TYPE));
		assertEquals("foo", consumerConfig.getString(Constants.PROP_DESTINATION_NAME));
		
		System.out.println(handler.getConfigString("AbstractConsumer configuration: ", consumerConfig));
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#usage()}.
	 */
	@Test
	public final void testUsage() {
		ConfigHandler handler = ConfigHandler.instance("executive");
		handler.usage();
	}

	/**
	 * Test method for {@link com.tibco.mcqueary.jmsperf.ConfigHandler#help()}.
	 */
	@Test
	public final void testHelp() {
		ConfigHandler handler=ConfigHandler.instance(KEY_ALL);
		handler.help();
	}

}
