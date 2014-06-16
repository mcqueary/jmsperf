package com.tibco.mcqueary.jmsperf;

import org.kohsuke.args4j.Option;

public class ConsumerOptions extends CommonOptions {

	private final static String DEFAULT_DURABLE_NAME = null;
	@Option(name = "-durable", metaVar = "<mode>",
			usage = "Durable subscription name. No default.")	
	private String durable=DEFAULT_DURABLE_NAME;

	protected String getDurable() {
		return durable;
	}

	protected void setDurable(String durable) {
		this.durable = durable;
	}

	protected String getSelector() {
		return selector;
	}

	protected void setSelector(String selector) {
		this.selector = selector;
	}

	protected String getAckMode() {
		return ackMode;
	}

	protected void setAckMode(String ackMode) {
		this.ackMode = ackMode;
	}

	private final static String DEFAULT_SELECTOR = null;
	@Option(name = "-selector", metaVar = "<string>",
			usage = "Message selector for consumers. No default.")	
	private String selector=DEFAULT_SELECTOR;
	
	private final static String DEFAULT_ACK_MODE = "AUTO_ACKNOWLEDGE";
	@Option(name = "-ackmode", metaVar = "<string>",
			usage = "Message selector for consumers (default: "+DEFAULT_ACK_MODE + ")")	
	private String ackMode=DEFAULT_ACK_MODE;
	
	public ConsumerOptions(String[] args) {
		// TODO Auto-generated constructor stub
		super(args);
	}
	
	protected void init()
	{
		System.err.println("in " + this.getClass() + " init");
		super.init();
		setDurable(props.getProperty(OPT_CONSUMER_DURABLE_NAME));
		setAckMode(props.getProperty(OPT_CONSUMER_ACK_MODE));
		setSelector(props.getProperty(OPT_CONSUMER_SELECTOR));
		parseCommandLineArgs();
		printInitialValues(System.err);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConsumerOptions v = new ConsumerOptions(args);
	}

}
