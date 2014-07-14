package com.tibco.mcqueary.jmsperf;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
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
public class ConfigHandler implements Comparator<Option> {
	protected static final String PROJECT = "jmsperf";
	protected static final String PROJECT_PROPERTIES = PROJECT + ".properties";

	public static final String KEY_EXECUTIVE = "executive";
	public static final String KEY_CONSUMER = "consumer";
	public static final String KEY_PRODUCER = "producer";
	public static final String KEY_COMMON = "common";
	public static final String KEY_ALL = "all";

	public static final String OPT_CONFIG = "config";
	public static final String OPT_TOPIC = "topic";
	public static final String OPT_QUEUE = "queue";

	public static String OPT = "opt";
	public static String LONGOPT = "longopt";
	public static String LONGOPT_SUFFIX = "." + LONGOPT;
	public static String REQUIRED = "required";
	public static String NUMARGS = "numargs";
	public static String ARGNAME = "argname";
	public static String DESCRIPTION = "description";
	public static String VALID_VALUES = "valid.values";
	public static String MIN = "min";
	public static String MAX = "max";
	public static String INDEX = "index";
	public static String TYPE = "type";

	public static String WHITESPACE = "[\\s,;\\n\\t]+";

	private static Logger logger = Logger.getLogger(ConfigHandler.class);
	private static PropertiesConfiguration projectConfig = null;
	private PropertiesConfiguration instanceConfig = null;

	private Options options = null;
	private ArrayList<String> propNames = null;
	private String instName = null;

	public final class DestType {
		public static final String TOPIC = "topic";
		public static final String QUEUE = "queue";
	};

	// maps an option name to its corresponding property name.
	private Map<String, String> optionNamesMap = new HashMap<String, String>();

	private static Map<String, ConfigHandler> instMap = new HashMap<String, ConfigHandler>();

	/**
	 * A utility class for parsing a command line (using Commons CLI) and
	 * binding the command line option values to specified Properties. The
	 * binding is enabled by supplying a commons-cli PropertiesConfiguration
	 * with keys in the following form:
	 * 
	 * <property name> - The property to set from the <property name>.type - The
	 * name of the java type class. Currently supported: java.lang.Boolean,
	 * java.lang.Number. Default: java.lang.String command line option(s). The
	 * RHS value used as a default. <property name>.required - A boolean
	 * indicating whether or not this option is required <property name>.opt -
	 * The short option (single character string) <property name>.longopt - The
	 * long option (string) <property name>.numargs - Number of arguments
	 * <property name>.argname - Symbolic name for arguments <property
	 * name>.description - Description of the command line <property
	 * name>.valid.values - A delimited list of valid <property name>.min -
	 * Minimum numeric value, if any <property name>.max - Maximum numberic
	 * value, if any option.<name>.index - An int value used by HelpFormatter to
	 * determine help menu order.
	 * 
	 * @see Option, PropertiesConfiguration
	 * 
	 *      PROPERTY_NAME (property.name) - the name of the
	 */
	private ConfigHandler(String key) {
		this.instName = key;
		this.optionNamesMap = mapOptionNames();
		this.instanceConfig = getInstanceConfig();
		this.propNames = this.getPropertyNames();
		this.options = this.getOptions();
	}

	private Map<String, String> mapOptionNames() {
		HashMap<String, String> theMap = new HashMap<String, String>();

		List<String> keys = getKeysMatching(".*\\." + LONGOPT);
		for (Iterator<String> it = keys.iterator(); it.hasNext();) {
			String key = it.next();
			String optionName = projectConfig.getString(key);
			String propertyName = key.replaceAll("\\." + LONGOPT, "");
			// System.err.println("Putting [" + optionName +
			// "],["+propertyName+"] in option names map" );
			theMap.put(optionName, propertyName);
		}
		return theMap;
	}

	public static ConfigHandler instance(String key) {
		ConfigHandler inst = instMap.get(key);
		if (inst == null) {
			projectConfig = getProjectConfiguration();
			logger.trace("Creating new ConfigHandler for " + key);
			inst = new ConfigHandler(key);
			instMap.put(key, inst);
		}
		return inst;
	}

	public static PropertiesConfiguration getProjectConfiguration() {
		if (projectConfig == null) {
			try {
				projectConfig = new PropertiesConfiguration(PROJECT_PROPERTIES);
			} catch (ConfigurationException e) {
				System.err.println(e.getMessage());
			}
		}
		return projectConfig;
	}

	public ArrayList<String> getPropertyNames() {
		if (this.propNames == null) {
			ArrayList<String> pNames = new ArrayList<String>();
			String propNamesString = projectConfig.getString(instName
					+ ".options");
			String[] propNamesArray = propNamesString.split(WHITESPACE);
			for (String s : propNamesArray)
				pNames.add(s);

			this.propNames = pNames;
		}
		return this.propNames;
	}

	public String getPropertyDescription(String propertyName) {
		return projectConfig.getString(propertyName + "." + DESCRIPTION);
	}

	public Object[] getValidValues(String propertyName) {
		return projectConfig.getStringArray(propertyName + "." + VALID_VALUES);
	}

	/**
	 * @return The default properties defined in the project properties for this
	 *         client type
	 */
	public PropertiesConfiguration getInstanceConfig() {
		if (instanceConfig == null) {
			instanceConfig = new PropertiesConfiguration();
			String propNamesStr = projectConfig
					.getString(instName + ".options");
			List<String> propNames = Arrays.asList(propNamesStr
					.split(WHITESPACE));
			for (Iterator<String> keys = propNames.iterator(); keys.hasNext();) {
				String key = keys.next();
				instanceConfig.setProperty(key, projectConfig.getProperty(key));
			}
			listConfig("instanceConfig built for instance "+ instName, instanceConfig);
		}
		return instanceConfig;
	}

	public void updateInstanceConfig(Configuration defaults) {
		if (this.instanceConfig == null)
			getInstanceConfig();

		if (defaults != null) {
			for (String key : getPropertyNames()) {
				if (defaults.containsKey(key)) {
					this.instanceConfig.setProperty(key,
							defaults.getProperty(key));
				}
			}
		}
	}

//	@SuppressWarnings("unchecked")
//	public PropertiesConfiguration updateInstanceConfig(CommandLine line)
//			throws ParseException {
//
//		if (this.instanceConfig == null)
//			getInstanceConfig();
//
//		for (Iterator<Option> it = (Iterator<Option>) line.iterator(); it
//				.hasNext();) {
//			Option option = it.next();
//			if (this.getPropertyName(option) != null) {
//				String propName = this.getPropertyName(option);
//				String longOpt = option.getOpt();
//				Object value = option.getType();
//				if (value instanceof Number)
//					value = (Integer) line.getParsedOptionValue(longOpt);
//				else if (value instanceof Boolean)
//					value = (Boolean) line.getParsedOptionValue(longOpt);
//				else
//					value = (String) line.getOptionValue(longOpt);
//
//				value = this.validate(longOpt, value);
//
//				if (value == null)
//					this.instanceConfig.setProperty(propName, true);
//				else {
//					logger.debug("Setting " + propName + "=" + value);
//					this.instanceConfig.setProperty(propName, value);
//				}
//			}
//		}
//		return this.instanceConfig;
//	}

	private OptionGroup createOptionGroup(List<String> names, boolean required) {
		OptionGroup group = new OptionGroup();
		logger.debug("Creating option group for properties: " + names);
		for (String s : names) {
			Option o = createOption(s);
			group.addOption(o);
		}
		group.setRequired(required);
		logger.debug("Done creating option group for properties: " + names);
		return group;
	}

	/**
	 * Get the CLI Options for this configuration instance. The options and
	 * their associated properties are defined in the project .properties file.
	 * 
	 * @return the Options object for this instance class.
	 */
	public Options getOptions() {
		if (this.options == null) {
			this.options = new Options();

			logger.debug("In getOptions(" + this.instName + ")");

			List<String> workingPropertyList = new ArrayList<String>(
					getPropertyNames());

			String[] exclusiveOptions = projectConfig.getStringArray(instName
					+ ".options.exclusive");
			// for each exclusive group, with names delimited by pipe
			for (String s : exclusiveOptions) {
				// turn the group into a list of names
				List<String> exclusives = Arrays.asList(s.split("\\|"));

				// create a group of options from the names
				OptionGroup group = null;
				boolean required = projectConfig.getBoolean(instName
						+ ".options.exclusive.required", false);
				group = createOptionGroup(exclusives, required);

				// add the group to this instance's options
				options.addOptionGroup(group);

				// remove the options from the working list
				workingPropertyList.removeAll(exclusives);
			}

			// Add the remaining items singly
			for (String propName : workingPropertyList) {
				logger.trace("[" + propName + "="
						+ projectConfig.getProperty(propName) + "]");
				Option o = this.createOption(propName);
				if (projectConfig.containsKey(propName+"."+LONGOPT))
					options.addOption(o);
			}
		}
		return this.options;
	}

	private Option createOption(String propertyName) {
		Option option = null;

		String defaultValue = projectConfig.getString(propertyName);
		String opt = projectConfig.getString(propertyName + "." + OPT);
		String longOpt = projectConfig.getString(propertyName + "." + LONGOPT);
		String description = projectConfig.getString(propertyName + "."
				+ DESCRIPTION);
		Integer numArgs = projectConfig.getInteger(
				propertyName + "." + NUMARGS, 0);
		Boolean hasArg = (numArgs != null && numArgs > 0);
		String argName = projectConfig.getString(propertyName + "." + ARGNAME);
		Boolean required = projectConfig.getBoolean(propertyName + "."
				+ REQUIRED, false);
		String className = projectConfig.getString(propertyName + "." + TYPE,
				"java.lang.String");
		Object typeObject = null;

		option = new Option(longOpt, hasArg, description);
		if (hasArg) {
			option.setArgs(numArgs);
			if (argName != null) {
				option.setArgName(argName);
			}
		}
		option.setRequired(required);

		if (className != null && longOpt != null) {
			if (className.equals(Boolean.class.getName())) {
				typeObject = new Boolean(defaultValue);
			} else if (className.equals(Integer.class.getName())) {
				typeObject = new Integer(defaultValue);
			}
			if (typeObject != null) {
				option.setType(typeObject);
			}
		}
		logger.debug("Created option for inst=" + instName + " property "
				+ propertyName + ": " + className + "," + opt + "," + longOpt
				+ "," + hasArg + "," + argName + "," + description);

		return option;
	}

	public void printOptions(String msg, Options opts) {
		for (Iterator<Option> it = opts.getOptions().iterator(); it.hasNext();)
		{
			Option opt =it.next();
			msg += opt + "\n";
		}
		System.out.println(msg);
	}

	public String getPropertyName(Option option) {
		return optionNamesMap.get(option.getOpt());
	}

	public int getHelpIndex(Option opt) {
		int index = projectConfig.getInt(opt.getOpt() + ".index", 0);
		return index;
	}

	/**
	 * Given a propertyName, lookup its corresponding Option name.
	 * 
	 * @param propertyName
	 * @return The longOpt name associated with the given configuration property
	 *         name
	 */
	public String getOptionName(String propertyName) {
		return projectConfig.getString(propertyName + "." + LONGOPT);
	}

	private List<String> getKeysMatching(String regex) {
		List<String> retVal = new Vector<String>();

		for (Iterator<String> keys = projectConfig.getKeys(); keys.hasNext();) {
			String key = keys.next();
			if (key.matches(regex))
				retVal.add(key);
		}
		if (retVal.size() == 0)
			return null;
		else
			return retVal;
	}

	public List<String> getPropertyKeys(String category) {
		List<String> propertyKeys = null;

		String optionNames = projectConfig.getString(category + ".options");
		propertyKeys = Arrays.asList(optionNames.split(WHITESPACE));
		return propertyKeys;
	}

	/**
	 * @param propName
	 *            Name of the property being validated
	 * @param val
	 *            the value retrieved from the command line
	 * @return the value, or the default value
	 * @throws IllegalArgumentException
	 *             if the value isn't valid
	 */
	private Object validate(String propName, Object val)
			throws IllegalArgumentException {
		int min;
		int max;

		Configuration p = projectConfig.subset(propName);

		if (val instanceof Number) {
			Integer intVal = (Integer) val;
			if (p.containsKey(MIN) || p.containsKey(MAX)) {
				// numeric value
				if (p.containsKey(MIN)) {
					min = p.getInt(MIN);
					if (intVal < min) {
						throw new IllegalArgumentException(propName
								+ " must be >= " + min);
					}
				}
				if (p.getString(MAX) != null) {
					max = p.getInt(MAX);
					if (intVal > max) {
						throw new IllegalArgumentException(propName
								+ " must be <= " + max);
					}
				}
			}

		}

		if (p.containsKey(VALID_VALUES)) {
			List<Object> validList = p.getList(VALID_VALUES);
			if (!validList.contains(val)) {
				String msg = propName + " must be one of: ";
				for (Iterator<Object> i = validList.iterator(); i.hasNext();) {
					msg += i.next();
					if (i.hasNext())
						msg += ", ";
				}
				throw new IllegalArgumentException(msg);
			}
		}

		if (val == null)
			val = projectConfig.getString(propName);

		return val;

	}

	public static void listConfig(String msg, Configuration config) {
		logger.debug(getConfigString(msg, config));
	}

	public static String getConfigString(String msg, Configuration config) {
		String output = "\n==============" + msg + "==============\n";
		for (Iterator<String> it = config.getKeys(); it.hasNext();) {
			String key = it.next();
			output += key + "=[" + config.getProperty(key) + "]\n";
		}

		output += "==============" + msg + "==============";
		return output;
	}

	@Override
	public int compare(Option first, Option second) {
		String firstName = getPropertyName(first);
		String secondName = getPropertyName(second);
		int n = this.propNames.indexOf(firstName)
				- this.propNames.indexOf(secondName);
		return n;
	}

	public void printNames(PrintStream writer, List<String> names) {
		for (String s : names) {
			writer.print(s + ":" + names.indexOf(s) + " ");
		}
		writer.println();
	}

	final static String usageString = Executive.PROG_NAME
			+ " <-consumer|-producer> [ -config <filename> ] [OPTIONS]";

	void usage() {
		Options executiveOptions = instance("executive").getOptions();
		HelpFormatter formatter = new HelpFormatter();
		System.out.println("usage: " + usageString);
		PrintWriter pw = new PrintWriter(System.out, true);
		formatter.setOptionComparator(this);
		formatter.printOptions(pw, 80, executiveOptions, 5, 5);
	}

	void help() {
		int width = 100;
		int leftPad = 5;
		int descPad = 17;

		String syntaxString = usageString;

		// Options executiveOptions = instance("executive").getOptions();
		Options executiveOptions = instance("executive").getOptions();
		Options commonOptions = instance("common").getOptions();
		Options producerSpecificOptions = instance("producer.specific")
				.getOptions();
		Options consumerSpecificOptions = instance("consumer.specific")
				.getOptions();

		HelpFormatter formatter = new HelpFormatter();

		formatter.setOptPrefix("-");
		formatter.setLongOptPrefix("-");
		formatter.setOptionComparator(this);
		formatter.setWidth(width);
		formatter.setLeftPadding(leftPad);
		formatter.setDescPadding(descPad);

		PrintWriter pw = new PrintWriter(System.out, true);

		if (commonOptions != null) {
			formatter.setWidth(120);
			formatter.printHelp(syntaxString, " ", executiveOptions, null, false);
			formatter.setWidth(width);
		}

		if (commonOptions != null) {
			pw.println();
			pw.println("Common Options:");
			formatter.printOptions(pw, width, commonOptions, leftPad,
					descPad - 12);
		}

		if (consumerSpecificOptions != null) {
			pw.println();
			pw.println("Consumer Options:");
			formatter.printOptions(pw, width, consumerSpecificOptions, leftPad,
					descPad - 9);
		}

		if (producerSpecificOptions != null) {
			pw.println();
			pw.println("Producer Options:");
			formatter.printOptions(pw, width, producerSpecificOptions, leftPad,
					descPad - 10);
		}
	}

	@SuppressWarnings("unchecked")
	PropertiesConfiguration parseClientConfiguration(String[] args)
			throws ParseException {
		//remove our instance flag
		args = removeElementsMatching(args, "-+"+instName);
		CommandLineParser parser = new ExtendedGnuParser(false);
		Options options = this.getOptions();
		CommandLine line = parser.parse(options, args, false);

		/**
		 * If the user specified a config file, load that file and use its
		 * values as defaults.
		 */
		PropertiesConfiguration userSuppliedConfig = null;
		if (line.hasOption(OPT_CONFIG)) {
			String propFileName = line.getOptionValue(OPT_CONFIG);
			try {
				userSuppliedConfig = new PropertiesConfiguration(propFileName);
				// Add the user supplied config to the base configuration
				updateInstanceConfig(userSuppliedConfig);
				logger.debug("Loaded properties from " + propFileName);
			} catch (ConfigurationException e) {
				System.err.println(e.getMessage());
				logger.error(e.getMessage());
			}
		}
		
		// Now set values passed from command line, if there are any
		for (Iterator<Option> it = line.iterator(); it.hasNext();)
		{
			Option option = it.next();
			switch (option.getOpt())
			{
			case OPT_CONFIG:
				// ignore, already handled
				break;
			case OPT_TOPIC:
			case OPT_QUEUE:
				String destType=option.getOpt();
				String destName=line.getOptionValue(destType);
				logger.debug("Setting destination type = " + destType);
				instanceConfig.setProperty(JMSClient.PROP_DESTINATION_TYPE, destType);
				logger.debug("Setting destination name = " + destName);
				instanceConfig.setProperty(JMSClient.PROP_DESTINATION_NAME, destName);
				break;
			default:
				String key = this.getPropertyName(option);
				String value = line.getOptionValue(option.getOpt());
				if (value==null) //Boolean
					value="true";
				logger.debug("Setting " + key + "=" + value);
				instanceConfig.setProperty(key, value);
				break;
			}
		}
		// Override with values from the command line
//		config = updateInstanceConfig(line);
		listConfig("parseClientConfiguration is returning this: ", instanceConfig);
		return this.instanceConfig;
	}

	public static String[] removeElementsMatching(String[] input, String pattern) {
	    String[] result = null;
	    List<String> resultList = new LinkedList<String>();
	    for (String item : input)
	    {
	        if(!item.matches(pattern))
	        {
	            resultList.add(item);
	        }
	    }
	    result = resultList.toArray(new String[0]);
	    return result;
	}
	

	public static void printArgs(String prefix, String[] args) {
		if (args == null)
			return;

		String msg = new String();
		if (prefix != null)
			msg += prefix;
		msg += Executive.PROG_NAME;
		for (String arg : args)
			msg += " " + arg;
		System.err.println(msg);
	}


}
