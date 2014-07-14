package com.tibco.mcqueary.jmsperf;

import java.lang.management.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.tibco.mcqueary.jmsperf.ConfigHandler.DestType;

import static com.tibco.mcqueary.jmsperf.Executive.PFX;

abstract class JMSClient implements Worker {
	public static final String PROP_JNDI_URL = Context.PROVIDER_URL;
	public static final String PROP_JNDI_USERNAME = Context.SECURITY_PRINCIPAL;
	public static final String PROP_JNDI_PASSWORD = Context.SECURITY_CREDENTIALS;
	public static final String PROP_JNDI_CONTEXT_FACTORY = Context.INITIAL_CONTEXT_FACTORY;

	public static final String PROP_PROVIDER = PFX+"provider.name";
	public static final String PROP_PROVIDER_URL = PFX+"provider.connection.url";
	public static final String PROP_PROVIDER_USERNAME = PFX+"provider.connection.username";
	public static final String PROP_PROVIDER_PASSWORD = PFX+"provider.connection.password";
	public static final String PROP_PROVIDER_CONTEXT_FACTORY = PFX
			+ "provider.context.factory";
	public static final String PROP_PROVIDER_CONNECTION_FACTORY = PFX
			+ "provider.connection.factory";

	public static final String PROP_CLIENT_CONNECTIONS = PFX + "client.connections";
	public static final String PROP_CLIENT_SESSIONS = PFX + "client.sessions";
	public static final String PROP_UNIQUE_DESTINATIONS = PFX + "unique.destinations";
	public static final String PROP_DESTINATION_TYPE = PFX + "destination.type";
	public static final String PROP_DESTINATION_NAME = PFX + "destination.name";
	public static final String PROP_PROVIDER_TOPIC_FORMAT = PFX
			+ "provider.topic.lookup.format";
	public static final String PROP_PROVIDER_QUEUE_FORMAT = PFX
			+ "provider.queue.lookup.format";
	public static final String PROP_MESSAGE_COUNT = PFX + "message.count";
	public static final String PROP_DURATION_SECONDS = PFX + "duration.seconds";
	public static final String PROP_REPORT_INTERVAL_SECONDS = PFX + "report.interval.seconds";
	public static final String PROP_REPORT_WARMUP_SECONDS = PFX + "report.warmup.seconds";
	public static final String PROP_TRANSACTION_XA = PFX + "transaction.xa";
	public static final String PROP_TRANSACTION_SIZE = PFX + "transaction.size";
	public static final String PROP_DEBUG = PFX + "debug";
	
	protected final static int MEGABYTE = (1024 * 1024);

	protected static enum Flavor {
		GENERIC, TIBEMS, KAAZING, WMQ, HORNETQ, SWIFTMQ, ACTIVEMQ, QPID, OPENMQ
	};

	protected final static Map<String, Flavor> flavors;
	static {
		Map<String, Flavor> fMap = new HashMap<String, Flavor>();
		fMap.put("GENERIC", Flavor.GENERIC);
		fMap.put("TIBEMS", Flavor.TIBEMS);
		fMap.put("KAAZING", Flavor.KAAZING);
		fMap.put("WMQ", Flavor.WMQ);
		fMap.put("HORNETQ", Flavor.HORNETQ);
		fMap.put("SWIFTMQ", Flavor.SWIFTMQ);
		fMap.put("ACTIVEMQ", Flavor.ACTIVEMQ);
		fMap.put("QPID", Flavor.QPID);
		fMap.put("OPENMQ", Flavor.OPENMQ);
		flavors = Collections.unmodifiableMap(fMap);
	}
	// private static final Logger logger =
	// Logger.getLogger(jmsPerfCommon.class);

	// common variables
	protected static Logger logger = Logger.getLogger(JMSClient.class);
	protected MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
	protected static Context jndiContext = null;
	protected PropertiesConfiguration config = null;

	private Vector<Connection> connVector;
	private Iterator<Connection> connVectorIterator;
	private int destIter = 0;

	// Common Parameters
	protected static Flavor flavor = Flavor.GENERIC;
	protected String brokerURL = null;
	protected String connectionFactoryName = null;
	protected String contextFactoryName = null;
	protected String destName = null;
	protected String destType = null;
	protected String destNameFormat = null;
	protected int connections = 1;
	protected int sessions = 1;
	protected int msgGoal = 0;
	protected String username = null;
	protected String password = null;
	protected boolean uniqueDests = false;
	protected boolean xa = false;
	protected int reportInterval = 0;
	protected int txnSize = 0;
	protected int duration = 0;
	protected boolean debug = false;
	
	
	ConcurrentHashMap<Long, StatRecord> statList = new ConcurrentHashMap<Long, StatRecord>();

	protected String shortClassName = this.getClass().getSimpleName();
	
	protected List<StatRecord> stats = new Vector<StatRecord>();

	protected JMSClient(PropertiesConfiguration input)
			throws IllegalArgumentException, NamingException {
		this.config = input;

		ConfigHandler.listConfig(shortClassName
				+ " constructor invoked with this configuration: ", this.config);

		RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
		org.apache.log4j.MDC.put("PID", rt.getName());

		try {
			debug = config.getBoolean(PROP_DEBUG,false);
			flavor = flavors
					.get(config.getString(PROP_PROVIDER));
			brokerURL = config.getString(PROP_PROVIDER_URL);
			contextFactoryName = config
					.getString(PROP_PROVIDER_CONTEXT_FACTORY);
			connectionFactoryName = config
					.getString(PROP_PROVIDER_CONNECTION_FACTORY);
			username = config.getString(PROP_PROVIDER_USERNAME);
			password = config.getString(PROP_PROVIDER_PASSWORD);
			connections = config.getInt(PROP_CLIENT_CONNECTIONS);
			uniqueDests = config.getBoolean(PROP_UNIQUE_DESTINATIONS);
			sessions = config.getInt(PROP_CLIENT_SESSIONS);
			msgGoal = config.getInt(PROP_MESSAGE_COUNT);
			duration = config.getInt(PROP_DURATION_SECONDS);
			destType = config.getString(PROP_DESTINATION_TYPE);
			destName = config.getString(PROP_DESTINATION_NAME);
			switch (destType)
			{
			case DestType.TOPIC:
				destNameFormat = config.getString(PROP_PROVIDER_TOPIC_FORMAT);
				break;
			case DestType.QUEUE:
				destNameFormat = config.getString(PROP_PROVIDER_QUEUE_FORMAT);
				break;
			default:
				break;
			}
			destName = String.format(destNameFormat, destName);
			xa = config.getBoolean(PROP_TRANSACTION_XA, false);
			txnSize = config.getInt(PROP_TRANSACTION_SIZE);
			reportInterval = config.getInt(PROP_REPORT_INTERVAL_SECONDS);
		} catch (ConversionException e) {
			throw new IllegalArgumentException(e.getMessage(),e);
		}

		initJNDI(config);
	}

	public JMSClient(ClientBuilder builder) {
//		this.flavor = flavors.valueOf(builder.flavor);
	}

	public void setup() {
		// TODO 
	}

	protected void createConnectionFactoryAndConnections()
			throws NamingException, JMSException {
		// lookup the connection factory
		ConnectionFactory factory = null;
		if (this.connectionFactoryName != null) {
			factory = (ConnectionFactory) jndiLookup(connectionFactoryName);
		}

		if (factory == null) {
			logger.error("JNDI lookup failed for " + connectionFactoryName);
			return;
		}

		logger.info("Creating connections.");

		// create the connections
		this.connVector = new Vector<Connection>(this.connections);
		int i=0;
		try {
			for (; i < this.connections; i++) {
				Connection conn = null;
				conn = factory.createConnection(this.username, this.password);
				if (conn != null) {
//					if (conn.getClientID()==null)
//						conn.setClientID("connection:"+i);
					conn.start();
					this.connVector.add(conn);
					if ((i != 0) && (i % 1000) == 0) {
						logger.info(i + " of "
								+ connections + " connections created.");
					}
				}
			}
		} catch (JMSException e) {
			logger.error("Exception while creating connection #" + (i + 1)
					+ ": ", e);
			Exception linkedEx = ((JMSException) e).getLinkedException();
			if (null != linkedEx) {
				logger.error("Linked Exception:", linkedEx);
			}
		} catch (OutOfMemoryError e) {
			MemoryUsage m = this.memoryBean.getHeapMemoryUsage();
			logger.error("Exception encountered (" + this.connVector.size()
					+ " connections : Memory Use :" + m.getUsed() / MEGABYTE
					+ "M/" + m.getMax() / MEGABYTE + "M)", e);
		} catch (RuntimeException re) {
			logger.error("Runtime exception!", re);
		} finally {
			this.connVectorIterator = connVector.listIterator();
			logger.info(this.connVector.size() + " of " + connections
					+ " connections created.");
			if (this.connVector.size() < connections)
			{
				throw new JMSException("Couldn't create requested number of connections");
			}
		}
	}

	/**
	 * Returns a connection, synchronized because of multiple prod/cons threads
	 */
	protected synchronized Connection getConnection() {
		Connection conn = null;
		
		if (connVector.size() > 0)
		{
			if (!this.connVectorIterator.hasNext())
			{
				this.connVectorIterator = connVector.listIterator();
			}
			conn= (Connection)connVectorIterator.next();
		}
		
//		logger.trace("Connection #" + connVector.indexOf(conn) +
//				" clientID is " +  conn.getClientID());
		return conn;
	}

	protected void createXAConnectionFactoryAndXAConnections()
			throws NamingException, JMSException {
		// lookup the connection factory
		XAConnectionFactory factory = null;
		if (this.connectionFactoryName != null) {
			factory = (XAConnectionFactory) jndiLookup(this.connectionFactoryName);
		}

		// create the connections
		connVector = new Vector<Connection>(connections);
		for (int i = 0; i < connections; i++) {
			XAConnection conn = factory.createXAConnection(this.username,
					this.password);
			conn.start();
			this.connVector.add(conn);
		}
	}

	/**
	 * Returns a connection, synchronized because of multiple prod/cons threads
	 */
	protected synchronized XAConnection getXAConnection() {
		if (!this.connVectorIterator.hasNext())
			this.connVectorIterator = connVector.listIterator();

		return (XAConnection) connVectorIterator.next();
	}

	protected void cleanupConnections() throws JMSException {
		// close the connections
		for (int i = 0; i < this.connections; i++) {
			if (!xa) {
				Connection conn = (Connection) connVector.elementAt(i);
				conn.close();
			} else {
				XAConnection conn = (XAConnection) connVector.elementAt(i);
				conn.close();
			}
		}
	}

	/**
	 * Returns a destination, synchronized because of multiple prod/cons threads
	 */
	protected synchronized Destination getDestination(Session s)
			throws JMSException {
		// TODO The provider-specific name formatting should be handled better
		// TODO The name should be provided to the thread vs. having it do a
		// separate lookup.
		// TODO No more evil globals
		String name = destName;

		Destination dest = null;
		if (getUniqueDests()) {
			name = name + "." + ++destIter;
		}
		switch (getDestType())
		{
		case DestType.TOPIC:
			dest = s.createTopic(name);
			break;
		case DestType.QUEUE:
			dest = s.createQueue(name);
			break;
		default:
			break;
		}
			logger.trace("Created destination " + name + "("
				+ dest.getClass().getName() + ")");
		return dest;
	}

	/**
	 * Returns a txn helper object for beginning/committing transaction
	 * synchronized because of multiple prod/cons threads
	 */
	protected synchronized JMSPerfTxnHelper getPerfTxnHelper(boolean xa) {
		return new JMSPerfTxnHelper(xa);
	}

	/**
	 * Helper class for beginning/committing transactions, maintains any
	 * required state. Each prod/cons thread needs to get an instance of this by
	 * calling getPerfTxnHelper().
	 */
	protected class JMSPerfTxnHelper {
		public boolean startNewXATxn = true;
		public Xid xid = null;
		public boolean xa = false;

		public JMSPerfTxnHelper(boolean xa) {
			this.xa = xa;
		}

		protected void beginTx(XAResource xaResource) throws JMSException {
			if (xa && startNewXATxn) {
				/* create a transaction id */
//				java.rmi.server.UID uid = new java.rmi.server.UID();
				// this.xid = new com.tibco.tibjms.TibjmsXid(0, uid.toString(),
				// "branch");

				/* start a transaction */
				try {
					xaResource.start(xid, XAResource.TMNOFLAGS);
				} catch (XAException e) {
					logger.error("XAException: " + " errorCode=" + e.errorCode,
							e);
					System.exit(0);
				}
				startNewXATxn = false;
			}
		}

		protected void commitTx(XAResource xaResource, Session session)
				throws JMSException {
			if (xa) {
				if (xaResource != null && xid != null) {
					/* end and prepare the transaction */
					try {
						xaResource.end(xid, XAResource.TMSUCCESS);
						xaResource.prepare(xid);
					} catch (XAException e) {
						logger.error("XAException: " + " errorCode="
								+ e.errorCode, e);

						Throwable cause = e.getCause();
						if (cause != null) {
							logger.error("cause: ", cause);
						}

						try {
							xaResource.rollback(xid);
						} catch (XAException re) {
						}

						System.exit(0);
					}

					/* commit the transaction */
					try {
						xaResource.commit(xid, false);
					} catch (XAException e) {
						logger.error("XAException: " + " errorCode="
								+ e.errorCode, e);
						Throwable cause = e.getCause();
						if (cause != null) {
							logger.error("cause: ", cause);
						}

						System.exit(0);
					}
					startNewXATxn = true;
					xid = null;
				}
			} else {
				session.commit();
			}
		}
	}

	/**
	 * Constructs the JNDI context for this application. Strategy is (in order):
	 * 1. Inherit any JNDI properties if set<br>
	 * 2. Overwrite with project JNDI properties if set<br>
	 * 3. Use broker URL, username, and password for respective JNDI properties<br>
	 * 
	 * @param config
	 * @throws NamingException, IllegalArgumentException 
	 */
	public void initJNDI(Configuration config) throws NamingException, IllegalArgumentException {

		if (jndiContext != null)
			return;
	
		Configuration jndi = new PropertiesConfiguration();
		Configuration sysConfig = ConfigurationConverter.getConfiguration(System.getProperties());
		
		// Inherit any System JNDI properties
		for (Iterator<String> keys=sysConfig.getKeys("java.naming"); keys.hasNext();)
		{
			String key = keys.next();
			jndi.setProperty(key, sysConfig.getString(key));
		}
		
		// Override with any project JNDI properties
		for (Iterator<String> keys=config.getKeys("java.naming"); keys.hasNext();)
		{
			String key = keys.next();
			jndi.setProperty(key, config.getString(key));				
		}
		
		// Must have an initial context factory
		if (!jndi.containsKey(Context.INITIAL_CONTEXT_FACTORY))
		{
			if (config.containsKey(PROP_PROVIDER_CONTEXT_FACTORY))
			{
				jndi.setProperty(Context.INITIAL_CONTEXT_FACTORY, config.getString(PROP_PROVIDER_CONTEXT_FACTORY));
			} else {
				throw new IllegalArgumentException("Property " + Context.INITIAL_CONTEXT_FACTORY + 
						" must be set in the supplied configuration");								
			}
		}
		
		// If a JNDI URL wasn't provided, assume it's the same as the broker URL.
		if (!jndi.containsKey(Context.PROVIDER_URL))
			jndi.setProperty(Context.PROVIDER_URL,config.getString(PROP_PROVIDER_URL));
		
		// If a JNDI username/password weren't provided, assume they're the same as the broker
		if (!jndi.containsKey(Context.SECURITY_PRINCIPAL))
			jndi.setProperty(Context.SECURITY_PRINCIPAL, config.getString(PROP_PROVIDER_USERNAME));
		if (!jndi.containsKey(Context.SECURITY_CREDENTIALS))
			jndi.setProperty(Context.SECURITY_CREDENTIALS, config.getString(PROP_PROVIDER_PASSWORD));

		Properties jndiProps = ConfigurationConverter.getProperties(jndi);

		jndiContext = new InitialContext(jndiProps);
	}

	public static Object jndiLookup(String objectName) throws NamingException {
		if (objectName == null)
			throw new IllegalArgumentException("null object name not legal");

		if (objectName.length() == 0)
			throw new IllegalArgumentException("empty object name not legal");

		/*
		 * do the lookup
		 */
		if (jndiContext == null)
			logger.error("jndiContext is null");
		return jndiContext.lookup(objectName);
	}

	public PropertiesConfiguration getConfig() {
		return this.config;
	}

	/**
	 * @return the flavor
	 */
	protected static synchronized Flavor getFlavor() {
		return flavor;
	}

	/**
	 * @param flavor
	 *            the flavor to set
	 */
	protected static synchronized void setFlavor(Flavor flavor) {
		JMSClient.flavor = flavor;
	}

	/**
	 * @return the brokerURL
	 */
	protected synchronized String getBrokerURL() {
		return brokerURL;
	}

	/**
	 * @param brokerURL
	 *            the brokerURL to set
	 */
	protected synchronized void setBrokerURL(String jndiProviderURL) {
		this.brokerURL = jndiProviderURL;
	}

	/**
	 * @return the connectionFactoryName
	 */
	protected synchronized String getConnectionFactoryName() {
		return connectionFactoryName;
	}

	/**
	 * @param connectionFactoryName
	 *            the connectionFactoryName to set
	 */
	protected synchronized void setConnectionFactoryName(
			String connectionFactoryName) {
		this.connectionFactoryName = connectionFactoryName;
	}

	/**
	 * @return the contextFactoryName
	 */
	protected synchronized String getContextFactoryName() {
		return contextFactoryName;
	}

	/**
	 * @param contextFactoryName
	 *            the contextFactoryName to set
	 */
	protected synchronized void setContextFactoryName(String contextFactoryName) {
		this.contextFactoryName = contextFactoryName;
	}

	/**
	 * @return the destName
	 */
	protected synchronized String getDestName() {
		return destName;
	}

	/**
	 * @param destName
	 *            the destName to set
	 */
	protected synchronized void setDestName(String destName) {
		this.destName = destName;
	}

	/**
	 * @return the destNameFormat
	 */
	protected synchronized String getDestNameFormat() {
		return destNameFormat;
	}

	/**
	 * @param destNameFormat
	 *            the destNameFormat to set
	 */
	protected synchronized void setDestNameFormat(String destNameFormat) {
		this.destNameFormat = destNameFormat;
	}

	/**
	 * @return the actual number of connections established
	 */
	protected synchronized Integer getNumConnections() {
		return connVector.size();
	}

	/**
	 * @return the number of connections configured
	 */
	protected synchronized Integer getConnections() {
		return connections;
	}

	/**
	 * @param connections
	 *            the connections to set
	 */
	protected synchronized void setConnections(Integer connections) {
		this.connections = connections;
	}

	/**
	 * @return the sessions
	 */
	protected synchronized Integer getSessions() {
		return sessions;
	}

	/**
	 * @param sessions
	 *            the sessions to set
	 */
	protected synchronized void setSessions(Integer sessions) {
		this.sessions = sessions;
	}

	/**
	 * @return the msgGoal
	 */
	protected synchronized int getMsgGoal() {
		return msgGoal;
	}

	/**
	 * @param msgGoal
	 *            the msgGoal to set
	 */
	protected synchronized void setMsgGoal(int msgGoal) {
		this.msgGoal = msgGoal;
	}

	/**
	 * @return the username
	 */
	protected synchronized String getUsername() {
		return username;
	}

	/**
	 * @param username
	 *            the username to set
	 */
	protected synchronized void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	protected synchronized String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	protected synchronized void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the destType
	 */
	protected synchronized String getDestType() {
		return destType;
	}

	/**
	 * @param destType
	 *            the destType to set
	 */
	protected synchronized void setDestType(String destType) {
		this.destType = destType;
	}

	/**
	 * @return the uniqueDests
	 */
	protected synchronized Boolean getUniqueDests() {
		return uniqueDests;
	}

	/**
	 * @param uniqueDests
	 *            the uniqueDests to set
	 */
	protected synchronized void setUniqueDests(Boolean uniqueDests) {
		this.uniqueDests = uniqueDests;
	}

	/**
	 * @return the xa
	 */
	protected synchronized boolean isXa() {
		return xa;
	}

	/**
	 * @param xa
	 *            the xa to set
	 */
	protected synchronized void setXa(boolean xa) {
		this.xa = xa;
	}

	/**
	 * @return the reportInterval
	 */
	protected synchronized Integer getReportInterval() {
		return reportInterval;
	}

	/**
	 * @param reportInterval
	 *            the reportInterval to set
	 */
	protected synchronized void setReportInterval(Integer reportInterval) {
		this.reportInterval = reportInterval;
	}

	/**
	 * @return the txnSize
	 */
	protected synchronized int getTxnSize() {
		return txnSize;
	}

	/**
	 * @param txnSize
	 *            the txnSize to set
	 */
	protected synchronized void setTxnSize(int txnSize) {
		this.txnSize = txnSize;
	}

	/**
	 * @return the duration
	 */
	protected synchronized Integer getDuration() {
		return duration;
	}

	/**
	 * @param duration
	 *            the duration to set
	 */
	protected synchronized void setDuration(Integer duration) {
		this.duration = duration;
	}
	
	public static class ClientBuilder implements Builder<JMSClient>
	{
		protected int connections;
		protected int sessions;
		private String brokerURL;
		private String connectionFactoryName;
		private String contextFactoryName;
		private String destination;
		private String destType;
		private boolean uniquedests;
		private String flavor;
		private int msgGoal;
		private String username;
		private String password;
		private int duration;
		private int warmup;
		
		ClientBuilder()
		{
			
		}
		
		public ClientBuilder connections(int connections)
		{
			this.connections=connections;	return this;
		}
		
		public ClientBuilder sessions(int sessions)
		{
			this.sessions=sessions; return this;
		}
		
		public ClientBuilder brokerURL(String url)
		{
			this.brokerURL=url; return this;
		}
		public ClientBuilder connectionFactory(String factoryName)
		{
			this.connectionFactoryName= factoryName; return this;
		}
		public ClientBuilder contextFactory(String contextFactory)
		{
			this.contextFactoryName = contextFactory; return this;
		}
		public ClientBuilder destination(String dest)
		{
			this.destination=dest; return this;
		}
		public ClientBuilder destType(String destType)
		{
			this.destType= destType; return this;
		}
		public ClientBuilder uniqueDests(boolean unique)
		{
			this.uniquedests=unique; return this;
		}
		public ClientBuilder flavor(String flavor)
		{
			this.flavor = flavor; return this;
		}
		public ClientBuilder messages(int msgGoal)
		{
			this.msgGoal = msgGoal; return this;
		}
		
		public ClientBuilder username(String username)
		{
			this.username=username; return this;
		}
		public ClientBuilder password(String password)
		{
			this.password = password; return this;
		}
		public ClientBuilder duration(int duration)
		{
			this.duration = duration; return this;
		}
		public ClientBuilder warmup(int warmup)
		{
			this.warmup = warmup; return this;
		}
		
		@Override
		public JMSClient build() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
