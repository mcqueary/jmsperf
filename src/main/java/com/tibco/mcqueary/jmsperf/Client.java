package com.tibco.mcqueary.jmsperf;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import static com.tibco.mcqueary.jmsperf.Constants.*;

//	abstract class Client implements Worker {
abstract class Client {

	// common variables
	protected static Logger logger = Logger.getLogger(Client.class);
	protected MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
	protected static Context jndiContext = null;
	protected PropertiesConfiguration config = null;
	protected ConnectionFactory connectionFactory = null;
	
	private Vector<Connection> connVector;
	private Iterator<Connection> connVectorIterator;
	private int destIter = 0;

	// Common Parameters
	protected Provider provider = Provider.valueOf(PROP_PROVIDER_NAME_DEFAULT);
	protected String brokerURL = PROP_PROVIDER_URL_DEFAULT ;
	protected String connectionFactoryName = Constants.PROP_PROVIDER_CONNECTION_FACTORY_DEFAULT;
	protected String contextFactoryName = Constants.PROP_JNDI_CONTEXT_FACTORY_DEFAULT;
	protected String destName = Constants.PROP_DESTINATION_NAME_DEFAULT;
	protected DestType destType = DestType.TOPIC;
	protected String destNameFormat = Constants.PROP_PROVIDER_TOPIC_FORMAT_DEFAULT;
	protected int connections = Constants.PROP_CLIENT_CONNECTIONS_DEFAULT;
	protected int sessions = Constants.PROP_CLIENT_SESSIONS_DEFAULT;
	protected int msgGoal = Constants.PROP_MESSAGE_COUNT_DEFAULT;
	protected String username = Constants.PROP_PROVIDER_USERNAME_DEFAULT;
	protected String password = Constants.PROP_PROVIDER_PASSWORD_DEFAULT;
	protected boolean uniqueDests = Constants.PROP_UNIQUE_DESTINATIONS_DEFAULT;
	protected boolean xa = Constants.PROP_UNIQUE_DESTINATIONS_DEFAULT;
	protected int reportInterval = Constants.PROP_REPORT_INTERVAL_SECONDS_DEFAULT;
	protected int warmup = Constants.PROP_REPORT_WARMUP_SECONDS_DEFAULT;
	protected int offset = Constants.PROP_REPORT_OFFSET_MSEC_DEFAULT;
	protected int txnSize = Constants.PROP_TRANSACTION_SIZE_DEFAULT;
	protected int duration = Constants.PROP_DURATION_SECONDS_DEFAULT;
	protected boolean debug = Constants.PROP_DEBUG_DEFAULT;
	protected boolean transacted = false;
	protected final Set<AckMode> ackModes = EnumSet.noneOf(AckMode.class);
	
	ConcurrentHashMap<Long, StatRecord> statList = new ConcurrentHashMap<Long, StatRecord>();
	protected List<StatRecord> stats = new Vector<StatRecord>();

	protected String shortClassName = this.getClass().getSimpleName();
	
	protected Client(PropertiesConfiguration input)
			throws IllegalArgumentException, NamingException {
		this.config = input;

		ackModes.add(AckMode.AUTO_ACKNOWLEDGE);
		ackModes.add(AckMode.CLIENT_ACKNOWLEDGE);
		ackModes.add(AckMode.DUPS_OK_ACKNOWLEDGE);
		
		ConfigHandler.listConfig(shortClassName
				+ " constructor invoked with this configuration: ", this.config);

		RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
		org.apache.log4j.MDC.put("PID", rt.getName());

		try {
			debug = config.getBoolean(PROP_DEBUG, debug);
			provider = Provider.valueOf(config.getString(PROP_PROVIDER_NAME, provider.name()).toUpperCase());
			if (provider!=null)
			{
				config.setProperty(PROP_PROVIDER_TOPIC_FORMAT, provider.topicFormat());
				config.setProperty(PROP_PROVIDER_QUEUE_FORMAT, provider.queueFormat());
			}
			brokerURL = config.getString(PROP_PROVIDER_URL, provider.url());
			contextFactoryName = config
					.getString(PROP_PROVIDER_CONTEXT_FACTORY,provider.factory());
			connectionFactoryName = config
					.getString(PROP_PROVIDER_CONNECTION_FACTORY, connectionFactoryName);
			username = config.getString(PROP_PROVIDER_USERNAME, username);
			password = config.getString(PROP_PROVIDER_PASSWORD, password);
			connections = config.getInt(PROP_CLIENT_CONNECTIONS, connections);
			uniqueDests = config.getBoolean(PROP_UNIQUE_DESTINATIONS, uniqueDests);
			sessions = config.getInt(PROP_CLIENT_SESSIONS, sessions);
			msgGoal = config.getInt(PROP_MESSAGE_COUNT, msgGoal);
			duration = config.getInt(PROP_DURATION_SECONDS, duration);
			destType = DestType.valueOf(
					config.getString(PROP_DESTINATION_TYPE,destType.name()).toUpperCase());
			destName = config.getString(PROP_DESTINATION_NAME, destName);
			switch (destType)
			{
			case TOPIC:
				destNameFormat = config.getString(PROP_PROVIDER_TOPIC_FORMAT);
				break;
			case QUEUE:
				destNameFormat = config.getString(PROP_PROVIDER_QUEUE_FORMAT);
				break;
			default:
				break;
			}
			destName = String.format(destNameFormat, destName);
			xa = config.getBoolean(PROP_TRANSACTION_XA, xa);
			txnSize = config.getInt(PROP_TRANSACTION_SIZE, txnSize);
			transacted = (txnSize > 0);
			reportInterval = config.getInt(PROP_REPORT_INTERVAL_SECONDS, reportInterval);
			warmup = config.getInt(PROP_REPORT_WARMUP_SECONDS, warmup);
			offset = config.getInt(PROP_REPORT_OFFSET_MSEC, offset);
			
		} catch (ConversionException e) {
			throw new IllegalArgumentException(e.getMessage(),e);
		}

		initJNDI(config);
		if (this.connectionFactoryName != null) {
			connectionFactory = (ConnectionFactory) jndiLookup(connectionFactoryName);
			if (this.connectionFactory == null) {
				logger.error("JNDI lookup failed for " + connectionFactoryName);
				return;
			}
		}
	}

	public Client(Builder<?> builder) {
		this.provider = builder.provider;
		this.connections = builder.connections;
		this.sessions = builder.sessions;
		this.brokerURL = builder.brokerURL;
		this.connectionFactoryName = builder.connectionFactoryName;
		this.contextFactoryName = builder.contextFactoryName;
		this.destName = builder.destName;
		this.destType = builder.destType;
		this.uniqueDests = builder.uniqueDests;
		this.msgGoal = builder.msgGoal;
		this.username = builder.username;
		this.password = builder.password;
		this.duration = builder.duration;
		this.warmup = builder.warmup;
		this.offset = builder.offset;
		this.txnSize = builder.txnSize;
		this.xa = builder.xa;
		this.reportInterval = builder.reportInterval;
		this.debug = builder.debug;
	}

	public void setup() {
		// TODO 


	}

	protected void createConnections()
			throws NamingException, JMSException {
		
		logger.info("Creating connections using URL " + this.brokerURL);

		String intSpec = "%"+String.valueOf(connections).length()+"d";
		String format = intSpec + "/" + connections + " connections created/started.";

		// create the connections
		this.connVector = new Vector<Connection>(this.connections);
		int i=0;
		Connection conn = null;
		try {
			while (i < this.connections) {
				Thread.sleep(1);
				conn = this.connectionFactory.createConnection(this.username, this.password);
				if (conn != null) {
//					conn.start();
					++i;
					this.connVector.add(conn);
					if ((i % 1000) == 0) {
						logger.info(String.format(format, i));
					}
				}
			}
			format = intSpec + "/" + connections + " connections started.";
			for (i=0; i < this.connVector.size(); i++ )
			{
				conn = connVector.elementAt(i);
				conn.start();
				if ((i>0) && (i % 1000) == 0) {
					logger.info(String.format(format, i));
				}				
			}
		} catch (JMSException e) {
			logger.error(e.getMessage());
			logger.trace(e);
			Exception linkedEx = ((JMSException) e).getLinkedException();
			if (null != linkedEx) {
				logger.trace("Linked Exception:", linkedEx);
			}
		} catch (OutOfMemoryError e) {
			MemoryUsage m = this.memoryBean.getHeapMemoryUsage();
			logger.error("OutOfMemoryError encountered (" + this.connVector.size()
					+ " connections : Memory Use :" + m.getUsed() / MEGABYTE
					+ "M/" + m.getMax() / MEGABYTE + "M):");
			logger.error(e.getMessage());
		} catch (RuntimeException re) {
			logger.error("Runtime exception:", re);
		} catch (Exception e) {
			logger.error("Unexpected exception:", e);
		} finally {
			this.connVectorIterator = connVector.listIterator();
			logger.info(String.format(format, this.connVector.size()));
			if (this.connVector.size() < connections)
			{
				throw new JMSException("Couldn't create requested number of connections.");
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

		try {
			dest = (Destination) jndiLookup(name);
		} catch (NamingException e) {
		}
		
		if (dest == null)
		{
			switch (getDestType())
			{
			case TOPIC:
				dest = s.createTopic(name);
				break;
			case QUEUE:
				dest = s.createQueue(name);
				break;
			default:
				break;
			}
			logger.trace("Created destination " + name + "("
					+ dest.getClass().getName() + ")");
		}
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

	protected byte[] createPayload(int payloadSize) { return createPayload(null, payloadSize, payloadSize);}
	protected byte[] createPayload(String payloadFileName, int payloadSize) {
		return createPayload(payloadFileName, payloadSize, payloadSize);
	}
	/**
	 * Create the message.
	 */
	protected byte[] createPayload(String payloadFileName, int minSize, int maxSize) 
	{
		String payload = null;
		int bufferSize = 0;
		byte[] payloadBytes = null;
		int payloadSize=minSize;
		// create the message
//		BytesMessage msg = session.createBytesMessage();
		// add the payload
		if (payloadFileName != null) {
			try {
				InputStream instream = new BufferedInputStream(
						new FileInputStream(payloadFileName));
				bufferSize = instream.available();
				byte[] bytesRead = new byte[bufferSize];
				instream.read(bytesRead);
				instream.close();

				payload = new String(bytesRead);

				if (minSize > bufferSize) {
					logger.error("Payload file size (" + bufferSize
							+ ") < minimum msg size (" + maxSize + ")");
					logger.error("Exiting.");
					System.exit(-1);
				}

				if (maxSize > bufferSize) {
					logger.error("Payload file size (" + bufferSize
							+ ") < maximum msg size (" + maxSize
							+ "). Setting maximum msg size to " + bufferSize);
					maxSize = bufferSize;
				}

			} catch (IOException e) {
				logger.error("Error: unable to load payload file:", e);
			}
		} else if (payloadSize > 0) {
			StringBuffer msgBuffer = new StringBuffer(payloadSize);
			char c = 'A';
			for (int i = 0; i < payloadSize; i++) {
				msgBuffer.append(c++);
				if (c > 'z')
					c = 'A';
			}
			payload = msgBuffer.toString();
		}

		if (payload != null) {
//			logger.info("Creating message body (" + payload.length() + " bytes)");
			payloadBytes = payload.getBytes();
		}

		return payloadBytes;
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
		
		// Inherit any System JNDI properties
		Properties sysProps = System.getProperties();
		for (Iterator<Object> it = sysProps.keySet().iterator(); it.hasNext();)
		{
			String key = (String)it.next();
			if (key.matches("^java.naming\\..*"))
				jndi.setProperty(key, sysProps.get(key));
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

		logger.info("Initializing JNDI context with these properties: ");		
		for (Map.Entry <Object,Object> entry : jndiProps.entrySet())
		{
			if (entry.getValue()!=null)
				logger.info(entry.getKey() + ": " + entry.getValue());
		}
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

	protected void printConsoleBannerHeader()
	{
		System.err.println();
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println(this.getClass().getSimpleName());
		System.err
				.println("------------------------------------------------------------------------");
	}
	protected void printCommonSettings() {
		if (provider != null)
			System.err.println("Broker Provider.............. " + provider);
		System.err.println("Broker URL................... " + getBrokerURL());
		System.err.println("Initial Context Factory...... "
				+ contextFactoryName);
		System.err.println("Connection Factory........... "
				+ connectionFactoryName);
		System.err.println("User......................... " + username);
		System.err.println("Consumer Connections......... " + connections);
		System.err.println("Consumer Sessions............ " + sessions);		
		if (this.transacted) {
			System.err.println("Transaction Size............. " + getTxnSize());
			System.err.println("XA........................... " + xa);
		}
		System.err.println("Destination.................. " + "(" + destType
				+ ") " + destName);
		System.err.println("Unique Destinations.......... " + uniqueDests);
		System.err.println("Count........................ " + msgGoal);
		System.err.println("Duration..................... " + duration);
	}
	protected void printConsoleBannerFooter()
	{
		System.err
		.println("------------------------------------------------------------------------");
		System.err.println();
	}

	public PropertiesConfiguration getConfig() {
		return this.config;
	}

	/**
	 * @return the provider
	 */
	protected synchronized Provider getProvider() {
		return provider;
	}

	/**
	 * @param provider
	 *            the provider to set
	 */
	protected synchronized void setProvider(Provider provider) {
		this.provider = provider;
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
	protected synchronized DestType getDestType() {
		return this.destType;
	}

	/**
	 * @param destType
	 *            the destType to set
	 */
	protected synchronized void setDestType(DestType destType) {
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

	public static abstract class Builder<T extends Builder<T>>
	{
		private Provider provider = Provider.valueOf(PROP_PROVIDER_NAME_DEFAULT);
		private String brokerURL = PROP_PROVIDER_URL_DEFAULT ;
		private String connectionFactoryName = Constants.PROP_PROVIDER_CONNECTION_FACTORY_DEFAULT;
		private String contextFactoryName = Constants.PROP_JNDI_CONTEXT_FACTORY_DEFAULT;
		private String destName = Constants.PROP_DESTINATION_NAME_DEFAULT;
		private DestType destType = DestType.TOPIC;
		private String destNameFormat = Constants.PROP_PROVIDER_TOPIC_FORMAT_DEFAULT;
		private int connections = Constants.PROP_CLIENT_CONNECTIONS_DEFAULT;
		private int sessions = Constants.PROP_CLIENT_SESSIONS_DEFAULT;
		private int msgGoal = Constants.PROP_MESSAGE_COUNT_DEFAULT;
		private String username = Constants.PROP_PROVIDER_USERNAME_DEFAULT;
		private String password = Constants.PROP_PROVIDER_PASSWORD_DEFAULT;
		private boolean uniqueDests = Constants.PROP_UNIQUE_DESTINATIONS_DEFAULT;
		private boolean xa = Constants.PROP_UNIQUE_DESTINATIONS_DEFAULT;
		private int reportInterval = Constants.PROP_REPORT_INTERVAL_SECONDS_DEFAULT;
		private int warmup = Constants.PROP_REPORT_WARMUP_SECONDS_DEFAULT;
		private int offset = Constants.PROP_REPORT_OFFSET_MSEC_DEFAULT;
		private int txnSize = Constants.PROP_TRANSACTION_SIZE_DEFAULT;
		private int duration = Constants.PROP_DURATION_SECONDS_DEFAULT;
		private boolean debug = Constants.PROP_DEBUG_DEFAULT;
		protected Builder() {}
		
		protected abstract T me();
		
		public T connections(int connections)
		{
			this.connections=connections;	return me();
		}
		public T sessions(int sessions)
		{
			this.sessions=sessions; return me();
		}
		public T brokerURL(String url)
		{
			this.brokerURL=url; return me();
		}
		public T connectionFactory(String factoryName)
		{
			this.connectionFactoryName= factoryName; return me();
		}
		public T contextFactory(String contextFactory)
		{
			this.contextFactoryName = contextFactory; return me();
		}
		public T destination(String dest)
		{
			this.destName=dest; return me();
		}
		public T destType(DestType destType)
		{
			this.destType= destType; return me();
		}
		public T uniqueDests(boolean unique)
		{
			this.uniqueDests=unique; return me();
		}
		public T provider(Provider provider)
		{
			this.provider = provider; return me();
		}
		public T messages(int msgGoal)
		{
			this.msgGoal = msgGoal; return me();
		}
		public T username(String username)
		{
			this.username=username; return me();
		}
		public T password(String password)
		{
			this.password = password; return me();
		}
		public T duration(int duration)
		{
			this.duration = duration; return me();
		}
		public T warmup(int warmup)
		{
			this.warmup = warmup; return me();
		}
		public T reportInterval(int interval)
		{
			this.reportInterval = interval; return me();
		}
		public T debug(boolean debug)
		{
			this.debug = debug; return me();
		}
		public T xa(boolean isXA) {
			this.xa = isXA; return me();
		}
		public T transactionSize(int txnSize) {
			this.txnSize = txnSize; return me();
		}
		public T offset(int offset) {
			this.offset = offset; return me();
		}				
	}
}
