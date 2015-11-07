/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import static com.tibco.mcqueary.jmsperf.Constants.*;
import static com.tibco.mcqueary.jmsperf.ConfigHandler.*;

/**
 * @author Larry McQueary
 *
 */
public class Executive implements Runnable {
	// Constants
	protected static String versionInfo = null;
	private static Logger logger = Logger.getLogger(Executive.class.getName());

	protected static PropertiesConfiguration config = null;
	protected Client client = null;
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
	
	Executive(Client worker) {
		this.client = worker;
	}

	public Client getClient()
	{
		return this.client;
	}
	public Configuration getConfig() {
		return this.config;
	}

	public static String getVersionInfo() {
		if (versionInfo == null) {
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
		if (client != null) {
			client.setup();
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
							"Creating Producer with this configuration",
							config));
					executive = new Executive(new Producer(config));
//					executive = new Executive(handler.getProducer());
				break;
				case OPT_CONSUMER:
					handler = ConfigHandler.instance(ConfigHandler.KEY_CONSUMER);
					args = ConfigHandler.removeElementsMatching(args, "-+"+OPT_CONSUMER);
					config = handler.parseClientConfiguration(args);
					logger.debug(ConfigHandler.getConfigString(
							"Creating Consumer with this configuration",
							config));
					executive = new Executive(new Consumer(config));
//					executive = new Executive(handler.getConsumer());
					break;
				case OPT_REQUESTOR:
					handler = ConfigHandler.instance(ConfigHandler.KEY_REQUESTOR);
					args = ConfigHandler.removeElementsMatching(args, "-+"+OPT_REQUESTOR);
					config = handler.parseClientConfiguration(args);
					logger.debug(ConfigHandler.getConfigString(
							"Creating Requestor with this configuration",
							config));
					executive = new Executive(new Requestor(config));
//					executive = new Executive(handler.getConsumer());
					break;
				case OPT_RESPONDER:
					handler = ConfigHandler.instance(ConfigHandler.KEY_RESPONDER);
					args = ConfigHandler.removeElementsMatching(args, "-+"+OPT_RESPONDER);
					config = handler.parseClientConfiguration(args);
					logger.debug(ConfigHandler.getConfigString(
							"Creating Responder with this configuration",
							config));
					executive = new Executive(new Responder(config));
//					executive = new Executive(handler.getConsumer());
					break;
				default:
					break;
				}
				if (executive != null)
					executive.run();
				System.exit(0);
			}
		} catch (MissingOptionException e){
			List<?> missingOptions = e.getMissingOptions();
			String msg = PROG_NAME + ": ";
			List<String> names = new ArrayList<String>();
			
			for (Iterator missing = missingOptions.iterator(); missing.hasNext();) {
				Object missingOpt = missing.next();
				// Print out a single line for each required OptionGroup
				if (missingOpt instanceof OptionGroup) {
					
					msg += "at least one of the following must be selected: ";
					Iterator<String> it = ((OptionGroup)missingOpt).getNames().iterator();
					while (it.hasNext())
					{
						String s = it.next();
						msg += OPTION_PREFIX+s;
						if (it.hasNext())
							msg += ", ";
					}
					System.err.println(msg);
				}
				else
				{
					// Collect the names for each missing required (non-grouped) Option
					names.add((String)missingOpt);
				}
			}
			// finally, print the list of required single options, if any
			if (names.size() > 0)
			{
				msg = PROG_NAME + ": the following options are required: ";
				for (Iterator<String> i = names.iterator(); i.hasNext();) {
					String s = i.next();
					msg += s;
					if (i.hasNext())
						msg += ", ";
				}
				System.err.println(msg);
			}
			handler.usage();
		} catch (AlreadySelectedException e) {
			OptionGroup g = e.getOptionGroup();
			String msg = PROG_NAME + ": can only select one of ";
			for (Iterator<String> i = g.getNames().iterator(); i.hasNext();) {
				String s = i.next();
				msg += s;
				if (i.hasNext())
					msg += ", ";
			}
			System.err.println(msg);
			handler.usage();
		}
		catch (MissingArgumentException e)
		{
			System.err.println(PROG_NAME + ": " + e.getMessage());
			handler.usage();
		}
		catch (ParseException e) {
			System.err.println(PROG_NAME + ": " + e.getMessage() + "("+ e.getClass().getSimpleName() + ")");
			handler.usage();
		} catch (IllegalArgumentException e) { 
			System.err.println(e.getClass().getName() + ": " +e.getMessage());
			e.printStackTrace();
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
