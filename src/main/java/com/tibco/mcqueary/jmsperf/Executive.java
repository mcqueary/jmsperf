/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

import javax.naming.NamingException;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

/**
 * @author Larry McQueary
 *
 */
public class Executive implements Runnable {
	// Constants
	protected static final String PKG = "com.tibco.mcqueary.jmsperf";
	protected static final String PROG_NAME = "jmsperf";
//	private static final String MANAGER = "manager";
	protected static final String PFX = "";

	public static final String OPT_HELP = "help";
	public static final String OPT_VERSION = "version";
	public static final String OPT_CONSUMER = "consumer";
	public static final String OPT_PRODUCER = "producer";

	protected static int MEGABYTE = (1024 * 1024);

//	public static enum WorkerType {
//		PRODUCER("producer"), CONSUMER("consumer");
//
//		private final String text;
//
//		/**
//		 * @param text
//		 */
//		private WorkerType(final String text) {
//			this.text = text;
//		}
//
//		/*
//		 * (non-Javadoc)
//		 * 
//		 * @see java.lang.Enum#toString()
//		 */
//		@Override
//		public String toString() {
//			return text;
//		}
//	};

	protected static String versionInfo = null;
	private static Logger logger = Logger.getLogger(Executive.class.getName());

	protected static PropertiesConfiguration config = null;
	protected Worker worker = null;
	protected ConfigHandler handler = null;

	Executive(String[] args)
	{
		List<String> argList = Arrays.asList(args);
		for (String arg : argList)
		{
			if (arg.matches("-+"+OPT_PRODUCER))
			{
				args=ConfigHandler.removeElementsMatching(args, "-+"+OPT_PRODUCER);
				handler = ConfigHandler.instance(OPT_PRODUCER);
				config = handler.getInstanceConfig();
			}
			else if (arg.matches("-+"+OPT_CONSUMER))
			{
				args=ConfigHandler.removeElementsMatching(args, "-+"+OPT_CONSUMER);
			}
		}
	}
	
	Executive(Worker worker) {
		this.worker = worker;
	}

	public Worker getWorker()
	{
		return this.worker;
	}
	public Configuration getConfig() {
		return this.config;
	}

	public static String getVersionInfo() {
		if (versionInfo == null) {
			ConfigHandler handler = ConfigHandler.instance(ConfigHandler.KEY_EXECUTIVE);
			Configuration conf = ConfigHandler.getProjectConfiguration();
			versionInfo = conf.getString("application.title");
		}
		return versionInfo;
	}

	public static void printVersionInfo() {
		System.out.println(getVersionInfo());
	}

	@Override
	public void run() {
		if (worker != null) {
			worker.startup();
		}
	}

	/**
	 * @param args
	 * @throws ParseException
	 * @throws NamingException
	 * @throws ConfigurationException
	 * @throws IllegalArgumentException
	 */
	public static void main(String[] args) {
		// final int UNEXPECTED_FAILURE = -2;
		// final int BAD_ARGS = -1;
		// int exitStatus = 0;

		Executive executive = null;
//		ConfigHandler.printArgs("Invoked as: ", args);

		ConfigHandler handler = ConfigHandler.instance(ConfigHandler.KEY_EXECUTIVE);
		Options options = handler.getOptions();

		CommandLineParser parser = new ExtendedGnuParser(true);

		CommandLine line = null;
		try {
			line = parser.parse(options, args, false);

			for (Iterator<Option> it = line.iterator(); it.hasNext();) {
				Option o = it.next();
				
				switch (o.getOpt()) {
				case OPT_HELP:
					ConfigHandler.instance(ConfigHandler.KEY_ALL).help();
					break;
				case OPT_VERSION:
					printVersionInfo();
					break;
				case OPT_PRODUCER:
					handler = ConfigHandler.instance(ConfigHandler.KEY_PRODUCER);
					args = ConfigHandler.removeElementsMatching(args, "-+"+OPT_PRODUCER);
					config = handler.parseClientConfiguration(args);
					logger.debug(ConfigHandler.getConfigString(
							"Creating JMSProducer with this configuration",
							config));
					executive = new Executive(new JMSProducer(config));
					break;
				case OPT_CONSUMER:
					handler = ConfigHandler.instance(ConfigHandler.KEY_CONSUMER);
					args = ConfigHandler.removeElementsMatching(args, "-+"+OPT_CONSUMER);
					config = handler.parseClientConfiguration(args);
					logger.debug(ConfigHandler.getConfigString(
							"Creating JMSConsumer with this configuration",
							config));
					executive = new Executive(new JMSConsumer(config));
					break;
				default:
					break;
				}
				if (executive != null)
					executive.run();
				System.out.println("Exiting");
				System.exit(0);
			}
		} catch (ParseException e) {
			if (e instanceof AlreadySelectedException) {
				AlreadySelectedException ase = (AlreadySelectedException) e;
				OptionGroup g = ase.getOptionGroup();
				String msg = PROG_NAME + ": can only select one of ";
				for (Iterator<String> i = g.getNames().iterator(); i.hasNext();) {
					String s = i.next();
					msg += s;
					if (i.hasNext())
						msg += ", ";
				}
				System.err.println(msg);
			}
			handler.usage();
		} catch (IllegalArgumentException e) { 
			System.err.println(e.getMessage());
		} catch (NamingException e) {
			e.printStackTrace();
		}

	}

}
