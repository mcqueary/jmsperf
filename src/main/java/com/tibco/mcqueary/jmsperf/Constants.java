package com.tibco.mcqueary.jmsperf;

import javax.naming.Context;

public class Constants {

	private Constants() { }
	
	protected static final String PKG = "com.tibco.mcqueary.jmsperf";
	protected static final String PROG_NAME = "jmsperf";
	protected static final String PFX = "";

	public static final String OPT_HELP = "help";
	public static final String OPT_VERSION = "version";
	public static final String OPT_CONSUMER = "consumer";
	public static final String OPT_PRODUCER = "producer";
	public static final String OPT_REQUESTOR = "requestor";
	public static final String OPT_RESPONDER = "responder";

	public static final String PROP_JNDI_URL = Context.PROVIDER_URL;
	public static final String PROP_JNDI_URL_DEFAULT = null;
	
	public static final String PROP_JNDI_USERNAME = Context.SECURITY_PRINCIPAL;
	public static final String PROP_JNDI_USERNAME_DEFAULT = null;
	
	public static final String PROP_JNDI_PASSWORD = Context.SECURITY_CREDENTIALS;
	public static final String PROP_JNDI_PASSWORD_DEFAULT = null;
	
	public static final String PROP_JNDI_CONTEXT_FACTORY = Context.INITIAL_CONTEXT_FACTORY;
	public static final String PROP_JNDI_CONTEXT_FACTORY_DEFAULT = null;
	
	public static final String PROP_PROVIDER_NAME = PFX+"provider.name";
	public static final String PROP_PROVIDER_NAME_DEFAULT = Provider.ACTIVEMQ.name();
	
	public static final String PROP_PROVIDER_URL = PFX+"provider.connection.url";
//	public static final String PROP_PROVIDER_URL_DEFAULT = "vm://localhost?broker.persistent=false&broker.useJmx=false";
	public static final String PROP_PROVIDER_URL_DEFAULT = null;
	
	public static final String PROP_PROVIDER_USERNAME = PFX+"provider.connection.username";
	public static final String PROP_PROVIDER_USERNAME_DEFAULT = (String)null;
	
	public static final String PROP_PROVIDER_PASSWORD = PFX+"provider.connection.password";
	public static final String PROP_PROVIDER_PASSWORD_DEFAULT = null;

	public static final String 
		PROP_PROVIDER_CONTEXT_FACTORY = PFX + "provider.context.factory";
	public static final String PROP_PROVIDER_CONTEXT_FACTORY_DEFAULT = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
	
	public static final String 
		PROP_PROVIDER_CONNECTION_FACTORY = PFX + "provider.connection.factory";
	public static final String PROP_PROVIDER_CONNECTION_FACTORY_DEFAULT = "ConnectionFactory";

	public static final String PROP_CLIENT_CONNECTIONS = PFX + "client.connections";
	public static final int PROP_CLIENT_CONNECTIONS_DEFAULT = 1;

	public static final String PROP_CLIENT_SESSIONS = PFX + "client.sessions";
	public static final int PROP_CLIENT_SESSIONS_DEFAULT = 1;
	
	public static final String PROP_UNIQUE_DESTINATIONS = PFX + "unique.destinations";
	public static final boolean PROP_UNIQUE_DESTINATIONS_DEFAULT = false;

	public static final String PROP_DESTINATION_TYPE = PFX + "destination.type";
	public static final String PROP_DESTINATION_TYPE_DEFAULT = DestType.TOPIC.toString();
	
	public static final String PROP_DESTINATION_NAME = PFX + "destination.name";
	public static final String PROP_DESTINATION_NAME_DEFAULT = "jmsperf";

	public static final String 
		PROP_PROVIDER_TOPIC_FORMAT = PFX + "provider.topic.lookup.format";
	public static final String PROP_PROVIDER_TOPIC_FORMAT_DEFAULT = "%s";
	
	public static final String 
		PROP_PROVIDER_QUEUE_FORMAT = PFX + "provider.queue.lookup.format";
	public static final String PROP_PROVIDER_QUEUE_FORMAT_DEFAULT = "%s";

	public static final String PROP_MESSAGE_COUNT = PFX + "message.count";
	public static final int PROP_MESSAGE_COUNT_DEFAULT = 0;

	public static final String PROP_DURATION_SECONDS = PFX + "duration.seconds";
	public static final int PROP_DURATION_SECONDS_DEFAULT = 0;

	public static final String PROP_REPORT_INTERVAL_SECONDS = PFX + "report.interval.seconds";
	public static final int PROP_REPORT_INTERVAL_SECONDS_DEFAULT = 5;
	
	public static final String PROP_REPORT_WARMUP_SECONDS = PFX + "report.warmup.seconds";
	public static final int PROP_REPORT_WARMUP_SECONDS_DEFAULT = 0;
	
	public static final String PROP_REPORT_OFFSET_MSEC = PFX + "report.offset.msec";
	public static final int PROP_REPORT_OFFSET_MSEC_DEFAULT = 0;

	public static final String PROP_TRANSACTION_XA = PFX + "transaction.xa";
	public static final boolean PROP_TRANSACTION_XA_DEFAULT = false;

	public static final String PROP_TRANSACTION_SIZE = PFX + "transaction.size";
	public static final int PROP_TRANSACTION_SIZE_DEFAULT = 0;
	
	public static final String PROP_CONSUMER_SELECTOR = PFX
			+ "consumer.selector";
	public static final String PROP_CONSUMER_SELECTOR_DEFAULT = null;

	public static final String PROP_CONSUMER_DURABLE = PFX
			+ "consumer.durable.name";
	public static final String PROP_CONSUMER_DURABLE_DEFAULT = null;

	public static final String PROP_CONSUMER_ACK_MODE = PFX
			+ "consumer.acknowledgement.mode";
//	public static final String PROP_CONSUMER_ACK_MODE_DEFAULT = "AUTO_ACKNOWLEDGE";
	public static final AckMode PROP_CONSUMER_ACK_MODE_DEFAULT = AckMode.AUTO_ACKNOWLEDGE;
	
	public static final String PROP_PRODUCER_RATE = PFX + "producer.send.rate";
	public static final int PROP_PRODUCER_RATE_DEFAULT = 0;

	public static final String PROP_PRODUCER_COMPRESSION = PFX
			+ "producer.message.compression";
	public static final boolean PROP_PRODUCER_COMPRESSION_DEFAULT = false;

	public static final String PROP_PRODUCER_PAYLOAD_FILENAME = PFX
			+ "producer.payload.filename";
	public static final String PROP_PRODUCER_PAYLOAD_FILENAME_DEFAULT = null;

	public static final String PROP_PRODUCER_PAYLOAD_SIZE = PFX
			+ "producer.message.size";
	public static final int PROP_PRODUCER_PAYLOAD_SIZE_DEFAULT = 0;
	
	public static final String PROP_PRODUCER_PAYLOAD_MINSIZE = PFX
			+ "producer.message.size.minimum";
	public static final int PROP_PRODUCER_PAYLOAD_MINSIZE_DEFAULT = 0;
	
	public static final String PROP_PRODUCER_PAYLOAD_MAXSIZE = PFX
			+ "producer.message.size.maximum";
	public static final int PROP_PRODUCER_PAYLOAD_MAXSIZE_DEFAULT = 0;
			
	public static final String PROP_PRODUCER_DELIVERY_MODE = PFX
			+ "producer.delivery.mode";
	public static final String PROP_PRODUCER_DELIVERY_MODE_DEFAULT = "PERSISTENT";
			
	public static final String PROP_PRODUCER_TIMESTAMP = PFX
			+ "producer.message.timestamp";
	public static final boolean PROP_PRODUCER_TIMESTAMP_DEFAULT = false;

	public static final String PROP_PRODUCER_MESSAGEID = PFX
			+ "producer.enable.messageid";
	public static final boolean PROP_PRODUCER_MESSAGEID_DEFAULT = false;

	public static final String PROP_DEBUG = PFX + "debug";
	public static final boolean PROP_DEBUG_DEFAULT = false;
	
	protected final static int MEGABYTE = (1024 * 1024);
	
	public enum DestType {
		TOPIC, QUEUE;
	};


	public enum DeliveryMode {
		PERSISTENT(javax.jms.DeliveryMode.PERSISTENT),
		NON_PERSISTENT(javax.jms.DeliveryMode.NON_PERSISTENT);
		
		private final int value;
		
		DeliveryMode(int value)
		{
			this.value = value;
		}
		
		public int value() { return this.value; }	
	}
	
	public enum AckMode {
		AUTO_ACKNOWLEDGE(javax.jms.Session.AUTO_ACKNOWLEDGE),
		CLIENT_ACKNOWLEDGE(javax.jms.Session.CLIENT_ACKNOWLEDGE),
		DUPS_OK_ACKNOWLEDGE(javax.jms.Session.DUPS_OK_ACKNOWLEDGE),
		
		TIBCO_NO_ACKNOWLEDGE(22),
		TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE(23),
		TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE(24);
		
		private final int value;
		
		AckMode(int value)
		{
			this.value=value;
		}
		
		int value() { return this.value; }
	}
	
	public enum Provider {
		ACTIVEMQ("tcp://localhost:61616", "org.apache.activemq.jndi.ActiveMQInitialContextFactory"),
		HORNETQ("jnp://localhost:1099", "org.jboss.naming.remote.client.InitialContextFactory"),
		KAAZING("ws://localhost:8001/jms", "com.kaazing.gateway.jms.client.JmsInitialContextFactory", "/topic/%s", "/queue/%s"),
		QPID("qpid.jndi.properties","org.apache.qpid.jndi.PropertiesFileInitialContextFactory"),
		QPID_AMPQ("qpid-amqp.jndi.properties","org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory"),
		SWIFTMQ("smqp://localhost:4001", "com.swiftmq.jndi.InitialContextFactoryImpl"),
		TIBEMS("tibjmsnaming://localhost:7222", "com.tibco.tibjms.naming.TibjmsInitialContextFactory"); 
		
		//WMQ, HORNETQ, SWIFTMQ, QPID, OPENMQ 
		private final String defaultURL;
		private final String defaultContextFactory;
		private final String topicLookupFormat;
		private final String queueLookupFormat;
		Provider(String url, String factory) 
		{
			this(url, factory, null, null);
		}
		Provider(String url, String factory, String topicFormat, String queueFormat) 
		{
			this.defaultURL = url;
			this.defaultContextFactory = factory;
			if (topicFormat==null)
				this.topicLookupFormat=PROP_PROVIDER_TOPIC_FORMAT_DEFAULT;
			else
				this.topicLookupFormat = topicFormat;
			if (queueFormat==null)
				this.queueLookupFormat = PROP_PROVIDER_QUEUE_FORMAT_DEFAULT;
			else
				this.queueLookupFormat = queueFormat;
		}

		public String url() { return this.defaultURL; }
		public String factory() { return this.defaultContextFactory; }
		public String topicFormat() { return this.topicLookupFormat; }
		public String queueFormat() { return this.queueLookupFormat; }
		
	};

}
