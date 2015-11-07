package com.tibco.mcqueary.jmsperf;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;

import static com.tibco.mcqueary.jmsperf.Constants.*;

public abstract class AbstractConsumer extends Client implements Runnable {

	// Class-specific fields/parameters
	protected String selector = null;
	protected AckMode ackMode = Constants.PROP_CONSUMER_ACK_MODE_DEFAULT;
	protected String subscriptionName = null;
	protected String durableName = null;
	protected boolean async = false;

	// variables
	// protected AtomicLong currMsgCount = new AtomicLong(0);
	protected AtomicLong latencyTotal = new AtomicLong(0);
	protected AtomicLong elapsed = new AtomicLong(0);
	protected AtomicLong latencySamples = new AtomicLong(0);

	protected AtomicBoolean done = new AtomicBoolean(false);
	protected ReportManager reportManager;

	/**
	 * Constructor
	 * 
	 * @param inputConfig
	 *            - A PropertiesConfiguration object describing the default +
	 *            command-line configuration
	 * @throws ConfigurationException
	 * @throws NamingException
	 * @throws IllegalArgumentException
	 */
	public AbstractConsumer(PropertiesConfiguration inputConfig)
			throws IllegalArgumentException, NamingException {
		super(inputConfig);

		try {
			this.selector = config.getString(PROP_CONSUMER_SELECTOR, null);

			String ackModeString = config.getString(PROP_CONSUMER_ACK_MODE,
					PROP_CONSUMER_ACK_MODE_DEFAULT.name());
			this.ackMode = AckMode.valueOf(ackModeString);

			this.durableName = config.getString(PROP_CONSUMER_DURABLE, null);
		} catch (ConversionException ce) {
			throw new IllegalArgumentException(ce.getMessage(), ce);
		}
	}

	public AbstractConsumer(Builder builder) {
		super(builder);

		this.selector = builder.selector;
		this.durableName = builder.durable;
		this.ackMode = builder.ackMode;
	}

	protected void acknowledge(Message msg, int mode) throws JMSException {
		msg.acknowledge();
	}

	protected void process(Message msg) throws JMSException, IOException {
		// No-op. Override this to do provider-specific post processing

	}

	@Override
	public void setup() {
		try {
			createConnections();

			// create the reporting thread
			reportManager = new ReportManager(reportInterval, msgGoal,
					sessions, warmup, offset);
			Thread reportingThread = new WorkerThread(reportManager,
					"ReportingThread");
			reportingThread.start();

			ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors
					.newFixedThreadPool(sessions);
			pool.setThreadFactory(new JMSWorkerThreadFactory(
					"JMSConsumerThread"));
			pool.prestartAllCoreThreads();
			// create the consumer session threads

			JMSWorkerThread.setDebug(false);

			// SessionWorker[] workers = new SessionWorker[this.sessions];

			String intSpec = "%" + String.valueOf(this.sessions).length() + "d";
			String format = intSpec + "/" + this.sessions
					+ " session threads created.";

			for (int i = 0; i < this.sessions; i++) {
				// workers[i] = new SessionWorker(getConnection(), destName,
				// this.ackMode, this.txnSize > 0, this.selector,
				// this.subscriptionName, i);
				pool.execute(this);
				if ((i != 0) && (i % 1000) == 0)
					logger.info(String.format(format, i));
			}
			while (pool.getActiveCount() < this.sessions) {
				logger.info("Waiting for all sessions to activate.");
				Thread.sleep(1000);
			}
			logger.info(String.format(format, pool.getActiveCount()));
			if (duration > 0)
				logger.info("Waiting " + duration
						+ " sec for sessions to complete.");
			else
				logger.info("Waiting for sessions to complete.");

			while (((duration == 0) || (elapsed.get() < duration))
					&& ((msgGoal == 0) || (reportManager.getMsgCount() < msgGoal))) {
				int sleepSeconds = 1;
				Thread.sleep(sleepSeconds * 1000);
				elapsed.addAndGet(sleepSeconds);
			}

			done.set(true);
			reportManager.stopTiming();

			// for (int i=0; i<this.sessions; i++)
			// {
			// logger.debug("Cleaning up session[" + i + "], which processed "
			// + workers[i].getNumProcessed() + " msgs");
			// workers[i].cleanup();
			// }

			logger.debug("Shutting down active threads.");
			logger.debug("poolSize=" + pool.getPoolSize());
			logger.debug("corePoolSize=" + pool.getCorePoolSize());
			logger.debug("maximumPoolSize=" + pool.getMaximumPoolSize());
			logger.debug("largestPoolSize=" + pool.getLargestPoolSize());
			logger.debug("taskCount=" + pool.getTaskCount());
			logger.debug("completedTasks=" + pool.getCompletedTaskCount());
			logger.debug("keepAliveTime="
					+ pool.getKeepAliveTime(TimeUnit.MILLISECONDS) + "msec");

			// pool.shutdownNow();

			logger.debug("Shutting down reporting thread");
			reportingThread.interrupt();

			// close connections
			logger.debug("Cleaning up connections");
			cleanupConnections();

		} catch (NamingException | JMSException e) {
			logger.error(e.getMessage());
		} catch (OutOfMemoryError e) {
			long maxMemory = reportManager.getMaxMemory();
			long usedMemory = reportManager.getUsedMemory();
			logger.debug("\n\n(" + getNumConnections()
					+ " connections : Memory Use :" + usedMemory + "M/"
					+ maxMemory + "M)");
			logger.error("OutOfMemoryError during setup:", e);
		} catch (Exception e) {
			logger.error("Unexpected exception during setup:", e);
		} finally {
			logger.info("Done.");
		}
	}

	public void run() {

		long sessionNum = Thread.currentThread().getId();
		Connection connection = null;
		Destination destination = null;
		MessageConsumer msgConsumer = null;
		Session session = null;
		XAResource xaResource = null;
		JMSPerfTxnHelper txnHelper = getPerfTxnHelper(xa);
		int nProcessedThisThread = 0;

		try {
			/*
			 * unique topic destinations: total should be msgGoal *
			 * dests(sessions) unique queue destinations: total should be
			 * msgGoal * dests(sessions) single topic destination: total should
			 * be msgGoal * dests(sessions) single queue destination: total
			 * should be msgGoal
			 */

			// Get a connection from the "pool"
			connection = getConnection();

			// create a session
			session = connection.createSession(this.transacted,
					this.ackMode.value());

			logger.trace("Created session (" + this.transacted + ", " + ackMode
					+ ")");

			if (uniqueDests)
				this.destName = this.destName + "." + sessionNum;

			switch (destType) {
			case TOPIC:
				destination = session.createTopic(this.destName);
				break;
			case QUEUE:
				destination = session.createQueue(this.destName);
				break;
			default:
				break;
			}

			logger.trace("Created " + destType + " " + this.destName);

			// create the consumer
			if (this.durableName == null) {
				msgConsumer = session
						.createConsumer(destination, this.selector);
			} else
				msgConsumer = session.createDurableSubscriber(
						(Topic) destination, this.durableName, this.selector,
						false);
			logger.trace("Created consumer on " + destination);

			// Tracks whether we have looped at least once yet
			boolean started = false;

			while (!done.get()) {
				// a no-op for local txns
				// txnHelper.beginTx(xaResource);

				Message msg = msgConsumer.receive();
				if (msg == null) {
					logger.debug("NULL msg received");
					break;
				}
				if (!started) {
					reportManager.startTiming();
					started = true;
					logger.trace("Started processing");
				}
				// currMsgCount.incrementAndGet();
				reportManager.incrementMsgCount();
				nProcessedThisThread++;

				if (elapsed.get() > warmup)
					reportManager.recordLatency(0L, msg);

				/**
				 * Post-process hook for overriding. Default is no-op.
				 */
				process(msg);

				// statList.put(currMsgCount.get(),new StatRecord(msg,
				// this.connection, this.sessionNum));

				// commit the transaction if necessary
				if ((txnSize > 0)
						&& (reportManager.getMsgCount() % txnSize == 0))
					txnHelper.commitTx(xaResource, session);

				/**
				 * If explicit client acknowledgement (or subclass overrides)
				 * are required
				 */

				if ((this.ackMode != AckMode.AUTO_ACKNOWLEDGE)
						&& (this.ackMode != AckMode.DUPS_OK_ACKNOWLEDGE)) {
					// Allows for provider-specific ack modes
					acknowledge(msg, this.ackMode.value());
				}
			} // while loop
				// done.set(true);
			logger.debug("Message goal reached. This session (#" + sessionNum
					+ ") processed " + nProcessedThisThread);
		} catch (JMSException e) {
			e.printStackTrace();
			Exception le = e.getLinkedException();
			if (le == null || !(le instanceof InterruptedException)) {
				logger.error("Exception encountered in consumer thread: "
						+ e.getMessage());
				logger.error(e);
				logger.trace(e);
			}
		} catch (OutOfMemoryError e) {
			long max = reportManager.getMaxMemory();
			long used = reportManager.getUsedMemory();
			logger.error("OutOfMemory error in consumer thread (" + used + "M/"
					+ max + "M)", e);
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			logger.trace("Thread exiting after " + reportManager.getMsgCount()
					+ " total messages received (" + nProcessedThisThread
					+ " this thread)");
		}
	} // run()

	public void printConsoleBanner() {
		// print parameters

		printConsoleBannerHeader();
		printCommonSettings();
		printSpecificSettings();
		printConsoleBannerFooter();
	}

	public void printSpecificSettings() {
		if (durableName != null)
			System.err.println("Durable...................... " + durableName);
		System.err.println("Report Interval.............. " + reportInterval);
		System.err.println("Warmup Interval.............. " + warmup);
		System.err.println("Ack Mode..................... " + ackMode);
		if (selector != null)
			System.err.println("Selector..................... " + selector);
	}

	public static abstract class Builder<T extends Client.Builder<T>> extends Client.Builder<T>
	{
		private String selector = null;
		private String durable = null;
		private AckMode ackMode = PROP_CONSUMER_ACK_MODE_DEFAULT;

		@Override
		protected abstract T me();

		public T selector(String selector) {
			this.selector = selector; return me();
		}

		public T durable(String durable) {
			this.durable = durable;   return me();
		}

		public T ackMode(AckMode ackMode) {
			this.ackMode = ackMode;   return me();
		}

	}
}
