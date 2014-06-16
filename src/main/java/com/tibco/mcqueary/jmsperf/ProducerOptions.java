package com.tibco.mcqueary.jmsperf;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

//import com.tibco.mcqueary.jmsperf.CommonOptions.ProgramType;

public class ProducerOptions extends CommonOptions {

	
	private final static String DEFAULT_DELIVERY_MODE = "PERSISTENT";
	@Option(name = "-delivery", metaVar = "<mode>",
			usage = "Message delivery mode (default: " + DEFAULT_DELIVERY_MODE + ")")	
	private String deliveryMode=DEFAULT_DELIVERY_MODE;

	private final static int DEFAULT_RATE = 0;
	@Option(name = "-rate", metaVar = "<msg/sec>",
			usage = "Message rate for each producer thread. Default is " + DEFAULT_RATE)	
	private int rate=DEFAULT_RATE;

	private final static boolean DEFAULT_COMPRESSION = false;
	@Option(name = "-compression",
			usage = "Enable compression while sending msgs (default: " + DEFAULT_COMPRESSION + ")")	
	private boolean compression=DEFAULT_COMPRESSION;

	public ProducerOptions(String[] args)
	{
		super(args);
	}

	protected void init()
	{
		super.init();
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ProducerOptions v = new ProducerOptions(args);
		CmdLineParser parser = new CmdLineParser(v);
		parser.setUsageWidth(120);
		
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e)
		{
			System.err.println(e.getMessage());
			System.err.println("java ProducerOptions [options...] arguments...");
			// print the list of available options
			parser.printUsage(System.err);
			System.err.println();
			System.exit(1);
		}

	}

}
