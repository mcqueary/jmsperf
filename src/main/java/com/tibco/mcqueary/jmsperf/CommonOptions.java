package com.tibco.mcqueary.jmsperf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.kohsuke.args4j.*;

import com.tibco.mcqueary.jmsperf.jmsPerfCommon.Flavor;

public class CommonOptions {
	
	private String[] argList = null;
	
	protected static Properties props = null;
	protected String propFileName = null;
	
	//protected static enum DESTTYPE { QUEUE, TOPIC };

	// Option names
    protected static String DESTTYPE_TOPIC = "topic";
    protected static String DESTTYPE_QUEUE = "queue";
    
    protected static String OPT_PROVIDER_FLAVOR = "flavor";
    protected static String OPT_PROPERTY_FILE = null;
    public static String OPT_PROVIDER_JNDI = "java.naming.provider.url";
    public static String OPT_PROVIDER_ICF = "java.naming.factory.initial";
    protected static String OPT_USERNAME = "java.naming.security.principal";
    protected static String OPT_PASSWORD = "java.naming.security.credentials";

    protected static String OPT_FACTORY = "factory";
    protected static String OPT_DESTINATION_TYPE = "destination.type";
    protected static String OPT_DESTINATION_NAME = "destination.name";
    protected static String OPT_COUNT = "count";
    protected static String OPT_DURATION = "duration";
    
    protected static String OPT_COMPRESSION = "compression";
    protected static String OPT_UNIQUE_DESTS = "uniquedests";
    protected static String OPT_USE_XA = "use_xa";
    protected static String OPT_TXNSIZE = "txnsize";

    protected static String OPT_DEBUG = "debug";

    // Consumer options
    protected static String OPT_CONSUMER_THREADS = "consumer.threads";
    protected static String OPT_CONSUMER_CONNECTIONS = "consumer.connections";
    protected static String OPT_CONSUMER_DURABLE_NAME = "consumer.durable.name";
    protected static String OPT_CONSUMER_ACK_MODE = "consumer.ackmode";
    protected static String OPT_CONSUMER_SELECTOR = "consumer.selector";
    // Producer options
    protected static String OPT_PRODUCER_THREADS = "producer.threads";
    protected static String OPT_PRODUCER_CONNECTIONS = "producer.connections";
    protected static String OPT_PRODUCER_PAYLOAD_FILE = "producer.payload.file";
    protected static String OPT_PRODUCER_PAYLOAD_MINSIZE = "producer.payload.minsize";
    protected static String OPT_PRODUCER_PAYLOAD_MAXSIZE = "producer.payload.maxsize";
    protected static String OPT_PRODUCER_DELIVERY_MODE = "producer.delivery_mode";
    protected static String OPT_PRODUCER_MESSAGE_RATE = "producer.rate";
    

    protected static enum Flavor { TIBEMS, WMQ, HORNETQ, SWIFTMQ, ACTIVEMQ, QPID, OPENMQ };

	
	
	// Parameters
	private final static String DEFAULT_FLAVOR = "TIBEMS";
	@Option(name = "-flavor", metaVar = "<TIBEMS|WMQ>",
			usage = "Provider flavor (default: " + DEFAULT_FLAVOR + ")")	
	private Flavor flavor=Flavor.valueOf(DEFAULT_FLAVOR);
	
	@Option(name = "-propFile", metaVar = "<filename>",
			usage = "Name of properties file")	
	private String propFile=null;
	
	@Option(name = "-jndi", metaVar = "<url>",
			usage = "JNDI provider URL")	
	private String jndiProviderURL=null;
	
	@Option(name = "-user", aliases = { "-u" }, metaVar = "<username>",
			usage = "JNDI user name")	
	private String user=null;

	@Option(name = "-pass", aliases = { "-p" }, metaVar = "<password>",
			usage = "JNDI password")	
	private String password=null;

	private final static String DEFAULT_FACTORY = "ConnectionFactory";
	@Option(name = "-factory", metaVar = "<factory name>",
			usage = "Connection factory name (default: " + DEFAULT_FACTORY + ")")	
	private String factory=DEFAULT_FACTORY;

	private final static String DEFAULT_TOPIC = "topic.sample";
	@Option(name = "-topic", metaVar = "<topic name>",
			usage = "Name of topic (default: " + DEFAULT_TOPIC + ")")	
	private String topic=DEFAULT_TOPIC;

	private final static String DEFAULT_QUEUE = null;
	@Option(name = "-queue", metaVar = "<queue name>",
			usage = "Name of queue. No default.")	
	private String queue=DEFAULT_QUEUE;

	private final static int DEFAULT_COUNT = 10000;
	@Option(name = "-count", metaVar = "<# msgs>",
			usage = "Number of messages to send/receive (default: " + DEFAULT_COUNT + ")")	
	private int count=DEFAULT_COUNT;

	private final static int DEFAULT_TIME = 0;
	@Option(name = "-time", metaVar = "<# secs>",
			usage = "Number of seconds to run (default: " + DEFAULT_TIME + ")")	
	private int duration=DEFAULT_TIME;

	private final static int DEFAULT_CONNECTIONS = 1;
	@Option(name = "-connections", metaVar = "<# connections>",
			usage = "Number of connections to use (default: " + DEFAULT_CONNECTIONS +")")	
	private int connections=DEFAULT_CONNECTIONS;

	private final static int DEFAULT_THREADS = 1;
	@Option(name = "-threads", metaVar = "<# threads>",
			usage = "Number of threads to use (default: " + DEFAULT_THREADS + ")")	
	private int threads=DEFAULT_THREADS;

	private final static boolean DEFAULT_UNIQUEDESTS = false;
	@Option(name = "-uniquedests",
			usage = "Whether or not to use unique destinations (default: " + DEFAULT_UNIQUEDESTS + ")")	
	private boolean uniquedests=DEFAULT_UNIQUEDESTS;

	private final static int DEFAULT_TXNSIZE = 0;
	@Option(name = "-txnsize", metaVar = "<transaction size>",
			usage = "Number of messages per transaction (default: " + DEFAULT_TXNSIZE + ")")	
	private int txnSize=DEFAULT_TXNSIZE;

	
	public CommonOptions(String[] args) {
		// TODO Auto-generated constructor stub
		this.argList = args;
		init();
	}
	public CommonOptions() {
		// TODO Auto-generated constructor stub
	}

	protected static Properties getProps() {
		return props;
	}

	protected static void setProps(Properties props) {
		CommonOptions.props = props;
	}

	protected Flavor getFlavor() {
		return flavor;
	}

	protected void setFlavor(Flavor flavor) {
		this.flavor = flavor;
	}

	protected String getPropFile() {
		return propFile;
	}

	protected void setPropFile(String propFile) {
		this.propFile = propFile;
	}

	protected String getJndiProviderURL() {
		return jndiProviderURL;
	}

	protected void setJndiProviderURL(String jndiProviderURL) {
		this.jndiProviderURL = jndiProviderURL;
	}

	protected String getUser() {
		return user;
	}

	protected void setUser(String user) {
		this.user = user;
	}

	protected String getPassword() {
		return password;
	}

	protected void setPassword(String password) {
		this.password = password;
	}

	protected String getFactory() {
		return factory;
	}

	protected void setFactory(String factory) {
		this.factory = factory;
	}

	protected String getTopic() {
		return topic;
	}

	protected void setTopic(String topic) {
		this.topic = topic;
	}

	protected String getQueue() {
		return queue;
	}

	protected void setQueue(String queue) {
		this.queue = queue;
	}

	protected int getCount() {
		return count;
	}

	protected void setCount(int count) {
		this.count = count;
	}

	protected int getDuration() {
		return duration;
	}

	protected void setDuration(int duration) {
		this.duration = duration;
	}

	protected int getConnections() {
		return connections;
	}

	protected void setConnections(int connections) {
		this.connections = connections;
	}

	protected int getThreads() {
		return threads;
	}

	protected void setThreads(int threads) {
		this.threads = threads;
	}

	protected boolean isUniqueDests() {
		return uniquedests;
	}

	protected void setUniqueDests(boolean uniquedests) {
		this.uniquedests = uniquedests;
	}

	protected int getTxnSize() {
		return txnSize;
	}

	protected void setTxnSize(int txnSize) {
		this.txnSize = txnSize;
	}

	protected void parsePropertiesFile(String filename)
	{
    	Properties p = new Properties();
    	try {
    	  p.load(new FileInputStream(filename));
    	} catch (IOException e) {
    	  e.printStackTrace();
    	  return;
    	}
	}

	protected void printInitialValues(PrintStream outStream)
	{
		PrintStream out = outStream;
		if (out == null)
		{
			out = System.err;
		}
		out.println("Broker        ......................"+getFlavor());
		out.println("JNDI URL      ......................"+getJndiProviderURL());
		out.println("Factory       ......................"+getFactory());
		out.println("User          ......................"+getUser());
		out.println("Message Count ......................"+getCount());
		out.println("Duration      ......................"+getDuration());
		out.println("Connections   ......................"+getConnections());
		out.println("Threads       ......................"+getThreads());
		out.println("Unique Dests  ......................"+isUniqueDests());
		out.println("TXN Size      ----------------------"+getTxnSize());
		
	}
	
	protected void init()
	{
		String pFileName = getPropFileName();
		
		// load System (JVM) properties first
		props = System.getProperties();

		// Now read any specified property file
		if (pFileName != null)
		{
	    	try {
	      	  props.load(new FileInputStream(pFileName));
	      	} catch (IOException e) {
	      	  e.printStackTrace();
	      	}
		}

		Flavor f = null;
		String s = props.getProperty(OPT_PROVIDER_FLAVOR);
		if (s != null)
		{
			try {
				f = Flavor.valueOf(s);
				setFlavor(f);
			} catch (IllegalArgumentException e)
			{
				System.err.println("Ignoring illegal flavor value: " + s);
			}
		}
		setJndiProviderURL(props.getProperty(OPT_PROVIDER_JNDI));
		setFactory(props.getProperty(OPT_FACTORY, DEFAULT_FACTORY));
		setUser(props.getProperty(OPT_USERNAME, null));
		setPassword(props.getProperty(OPT_PASSWORD, null));
		// setTopic(props.getProperty(OPT_TOPIC_NAME, DEFAULT_TOPIC_NAME));
		// setQueue(props.getProperty(OPT_QUEUE_NAME, null));
		
		int num = Integer.parseInt(props.getProperty(OPT_COUNT, Integer.toString(DEFAULT_COUNT)));
		setCount(num);

		num=Integer.parseInt(props.getProperty(OPT_DURATION, Integer.toString(DEFAULT_TIME)));;
		setDuration(num);
		
		
		boolean b  = Boolean.parseBoolean(props.getProperty(OPT_UNIQUE_DESTS, Boolean.toString(false)));
		setUniqueDests(b);
		
		num = Integer.parseInt(props.getProperty(OPT_TXNSIZE, Integer.toString(DEFAULT_TXNSIZE)));
		setTxnSize(num);
		
		//props.list(System.err);
		
		//Now parse the caomand line
		parseCommandLineArgs();
		printInitialValues(System.err);
	}
	
    protected String getPropFileName()
    {
    	if (propFileName == null)
    	{
            int i=0;
            for (i=0; i < argList.length; i++)
            {
                if ((argList[i].compareToIgnoreCase("-propFile")==0)
                	|| (argList[i].compareToIgnoreCase("-f")==0))

                {
                    if ((i+1) >= argList.length)
                    {
                    	System.err.print("Must specify filename with -propFile options.");
                    	System.exit(-1);
                    }
                    propFileName = argList[i+1];
                    break;
                }
            }    		
    	}
        return propFileName;
    }
    
    void parseCommandLineArgs()
    {
		CmdLineParser parser = new CmdLineParser(this);
		parser.setUsageWidth(120);

		try {
			parser.parseArgument(argList);
		} catch (CmdLineException e)
		{
			System.err.println(e.getMessage());
			System.err.println("java CommonOptions [options...] arguments...");
			// print the list of available options
			parser.printUsage(System.err);
			System.err.println();
			System.exit(1);
		}
    }

	// For testing the class
	public static void main(String[] args)
	{
		CommonOptions v = new CommonOptions(args);
	}
}

