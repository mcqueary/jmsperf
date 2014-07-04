package com.tibco.mcqueary.jmsperf;

import java.lang.management.*;
import java.util.*;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

abstract class JMSWorker implements Worker {

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
	// private static final Logger LOGGER =
	// Logger.getLogger(jmsPerfCommon.class);

	// variables
	protected static Logger LOGGER = Logger.getLogger(JMSWorker.class);
	protected MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
	protected Vector<Connection> connsVector;
	protected static Context jndiContext = null;
	protected PropertiesConfiguration config = new PropertiesConfiguration();

	private int connIter = 0;
	private int destIter = 0;

	// Common Parameters
	protected static Flavor flavor = Flavor.GENERIC;
	protected String jndiProviderURL = null;
	protected String connectionFactoryName = null;
	protected String contextFactoryName = null;
	protected String destName = null;
	protected String destNameFormat = null;
	protected Integer connections = null;
	protected Integer sessions = null;
	protected Integer count = null;
	protected String username = null;
	protected String password = null;
	protected String durableName = null;
	protected String topicName = null;
	protected String queueName = null;
	protected String destType = null;
	protected Boolean uniqueDests = null;
	protected Boolean xa = null;
	protected Integer reportInterval = null;
	protected Integer txnSize = null;
	protected Integer runTime;
	
	protected JMSWorker(Configuration input) throws IllegalArgumentException {
		RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
		org.apache.log4j.MDC.put("PID", rt.getName());

		LOGGER.trace("In constructor for JMSWorker");
		try {
			// load default properties
			config.load("jms.properties");
			
			// update from provided configuration
			config.copy(input);

			JMSWorker.initJNDI(config);

			flavor = flavors.get(config.getString(Manager.PROP_PROVIDER_FLAVOR));
			jndiProviderURL = config.getString(Manager.PROP_PROVIDER_URL);
			contextFactoryName = config
					.getString(Manager.PROP_PROVIDER_CONTEXT_FACTORY);
			connectionFactoryName = config
					.getString(Manager.PROP_CONNECTION_FACTORY);
			username = config.getString(Manager.PROP_PROVIDER_USERNAME);
			password = config.getString(Manager.PROP_PROVIDER_PASSWORD);
			connections = config.getInt(Manager.PROP_CONNECTIONS);
			uniqueDests = config.getBoolean(Manager.PROP_UNIQUE_DESTINATIONS);
			sessions = config.getInt(Manager.PROP_SESSIONS);
			count = config.getInt(Manager.PROP_MESSAGE_COUNT);
			runTime = config.getInt(Manager.PROP_DURATION);
			destType = config.getString(Manager.PROP_DESTINATION_TYPE);
			destName = config.getString(Manager.PROP_DESTINATION_NAME);
			xa = config.getBoolean(Manager.PROP_TRANSACTION_XA);
			txnSize = config.getInt(Manager.PROP_TRANSACTION_SIZE);
		} catch (ConversionException e) {
			LOGGER.error(e.getMessage());
		} catch (ConfigurationException e) {
			LOGGER.error("Unable to load class properties: ", e);
		}
	}

	protected void createConnectionFactoryAndConnections()
			throws NamingException, JMSException {
		// lookup the connection factory
		ConnectionFactory factory = null;
		if (connectionFactoryName != null) {
			factory = (ConnectionFactory) jndiLookup(connectionFactoryName);
		}

		if (factory == null) {
			LOGGER.error("JNDI lookup failed for " + connectionFactoryName);
			return;
		}

		LOGGER.info("Creating " + connections + " connections...");

		// create the connections
		connsVector = new Vector<Connection>(connections);
		for (int i = 0; i < connections; i++) {
			Connection conn = null;
			try {
				conn = factory.createConnection(username, password);
				if (conn != null) {
					conn.start();
					connsVector.add(conn);
					if ((i > 0) && (i % 1000) == 0) {
						LOGGER.debug(i + " connections created.");
					}
				}

			} catch (JMSException e) {
				LOGGER.error("Exception while creating connection #" + (i + 1)
						+ ": ", e);
				Exception linkedEx = ((JMSException) e).getLinkedException();
				if (null != linkedEx) {
					LOGGER.error("Linked Exception:", linkedEx);
				}
				break;
			} catch (OutOfMemoryError e) {
				MemoryUsage m = memoryBean.getHeapMemoryUsage();
				LOGGER.error("Exception encountered (" + connsVector.size()
						+ " connections : Memory Use :" + m.getUsed()
						/ MEGABYTE + "M/" + m.getMax() / MEGABYTE + "M)", e);
				break;
			} catch (RuntimeException re) {
				LOGGER.error("Runtime exception!", re);
			}
		}
		LOGGER.info(connsVector.size() + " of " + connections
				+ " connections created.");
		return;
	}

	protected void createXAConnectionFactoryAndXAConnections()
			throws NamingException, JMSException {
		// lookup the connection factory
		XAConnectionFactory factory = null;
		if (connectionFactoryName != null) {
			factory = (XAConnectionFactory) jndiLookup(connectionFactoryName);
		}

		// create the connections
		connsVector = new Vector<Connection>(connections);
		for (int i = 0; i < connections; i++) {
			XAConnection conn = factory.createXAConnection(username, password);
			conn.start();
			connsVector.add(conn);
		}
	}

	protected void cleanup() throws JMSException {
		// close the connections
		for (int i = 0; i < this.connections; i++) {
			if (!xa) {
				Connection conn = (Connection) connsVector.elementAt(i);
				conn.close();
			} else {
				XAConnection conn = (XAConnection) connsVector.elementAt(i);
				conn.close();
			}
		}
	}

	/**
	 * Returns a connection, synchronized because of multiple prod/cons threads
	 */
	protected synchronized Connection getConnection() {
		Connection connection = (Connection) connsVector.elementAt(connIter++);
		if (connIter == connections)
			connIter = 0;
		return connection;
	}

	/**
	 * Returns a connection, synchronized because of multiple prod/cons threads
	 */
	protected synchronized XAConnection getXAConnection() {
		XAConnection connection = (XAConnection) connsVector
				.elementAt(connIter++);
		if (connIter == connections)
			connIter = 0;
		return connection;
	}

	/**
	 * Returns a destination, synchronized because of multiple prod/cons threads
	 */
	protected synchronized Destination getDestination(Session s)
			throws JMSException {

		String name = destName;

		Destination dest = null;
		if (uniqueDests) {
			name = name + "." + ++destIter;
		}
		if (destType == Manager.DestType.TOPIC) {
			name = String.format(destNameFormat, name);
			dest = s.createTopic(name);
		} else { // QUEUE
			name = String.format(destNameFormat, name);
			dest = s.createQueue(name);
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
				java.rmi.server.UID uid = new java.rmi.server.UID();
				// this.xid = new com.tibco.tibjms.TibjmsXid(0, uid.toString(),
				// "branch");

				/* start a transaction */
				try {
					xaResource.start(xid, XAResource.TMNOFLAGS);
				} catch (XAException e) {
					System.err.println("XAException: " + " errorCode="
							+ e.errorCode);
					e.printStackTrace();
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
						System.err.println("XAException: " + " errorCode="
								+ e.errorCode);
						e.printStackTrace();

						Throwable cause = e.getCause();
						if (cause != null) {
							System.err.println("cause: ");
							cause.printStackTrace();
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
						System.err.println("XAException: " + " errorCode="
								+ e.errorCode);
						e.printStackTrace();

						Throwable cause = e.getCause();
						if (cause != null) {
							System.err.println("cause: ");
							cause.printStackTrace();
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

	public static void initJNDI(Configuration config) {
		if (jndiContext != null)
			return;
		else {
			try {
				Properties jndiProps = config.getProperties("java.naming");
				for (Iterator<Object> key = jndiProps.keySet().iterator(); key
						.hasNext();) {
					String s = (String) key.next();
					System.err.println("JNDI prop: " + key.next() + "="
							+ jndiProps.getProperty(s));
				}
				jndiContext = new InitialContext(jndiProps);
			} catch (NamingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static Object jndiLookup(String objectName) throws NamingException {
		if (objectName == null)
			throw new IllegalArgumentException("null object name not legal");

		if (objectName.length() == 0)
			throw new IllegalArgumentException("empty object name not legal");

		/*
		 * do the lookup
		 */
		return jndiContext.lookup(objectName);
	}
}
