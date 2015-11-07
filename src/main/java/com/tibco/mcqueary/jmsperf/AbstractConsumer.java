package com.tibco.mcqueary.jmsperf;

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

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;

import static com.tibco.mcqueary.jmsperf.Executive.PFX;

public class JMSConsumer extends JMSClient {
	
	public static final String PROP_CONSUMER_SELECTOR = PFX
			+ "consumer.selector";
	public static final String PROP_CONSUMER_DURABLE = PFX
			+ "consumer.durable.name";
	public static final String PROP_CONSUMER_ACK_MODE = PFX
			+ "consumer.acknowledgement.mode";
	
	public class SessionWorker implements Runnable {
		private int sessionNum;
		private Connection connection = null;
		private String destName = null;
		private int ackMode = -1;
		private boolean transacted = false;
		private String selector = null;
		private String durable = null;
		private Destination destination;
		private MessageConsumer msgConsumer = null;
		private Session session=null;
		XAResource xaResource = null;
		JMSPerfTxnHelper txnHelper = getPerfTxnHelper(xa);
		private int nProcessedThisThread=0;
		
		SessionWorker(Connection connection, String destName, int ackMode,
				boolean transacted, String selector, String durable, int sessionNum) {
			this.sessionNum=sessionNum;
			this.connection = connection;
			this.destName = destName;
			this.ackMode = ackMode;
			this.transacted=transacted;
			this.selector=selector;
			this.durable = durable;
			
			// create a session
			try {
				session = this.connection.createSession(this.transacted, this.ackMode);
	
				logger.trace("Created session (" + this.transacted + ", " 
						+ ackModes.inverseBidiMap().get(this.ackMode) + ")");
	
				//TODO remove global
				if (uniqueDests)
					this.destName=this.destName + "." + this.sessionNum;
					
				if (destType.equals(ConfigHandler.DestType.TOPIC)) {
					this.destination = session.createTopic(this.destName);
				} else { // QUEUE
					this.destination = session.createQueue(this.destName);
				}
				logger.trace("Created " + destType + " " + this.destName);
	
				// create the consumer
				if (this.durable == null)
					msgConsumer = session.createConsumer(this.destination,
							this.selector);
				else
					msgConsumer = session.createDurableSubscriber(
							(Topic) this.destination, this.durable,
							this.selector, false);
				logger.trace("Created consumer on " + this.destination);
			
			} catch (JMSException e) {
				logger.debug("Exception in SessionWorker constructor:", e);
			}
		}

		private int getNumProcessed()
		{
			return this.nProcessedThisThread;
		}
		
		public void cleanup()
		{
			try {
				// commit any remaining messages
				if (txnSize > 0) {
						txnHelper.commitTx(this.xaResource, this.session);
				}
				
				if (this.durable != null && this.session !=null)
				{
					logger.trace("Unsubscribing durable: " + this.durable);
					this.session.unsubscribe(this.durable);
				}

				logger.trace("Closing consumer.");
				if (this.msgConsumer!=null)
					this.msgConsumer.close();

				if (session != null)
				{
					logger.trace("Closing session.");
					this.session.close();
				}
				
			} catch (JMSException e)
			{
				logger.error("Exception during session cleanup:", e);
			}
		}
		
		@Override
		public void run() {
			boolean started = false;
			try {
				/*
				 * unique topic destinations: total should be msgGoal *
				 * dests(sessions) unique queue destinations: total should be
				 * msgGoal * dests(sessions) single topic destination: total
				 * should be msgGoal * dests(sessions) single queue destination:
				 * total should be msgGoal
				 */

//				while (!done.get() && (currMsgCount.get() < msgGoal))
				while (!done)
				{
					// a no-op for local txns
					// txnHelper.beginTx(xaResource);

					Message msg = msgConsumer.receive();
					if (msg == null)
					{
						logger.debug("NULL msg received");
						break;
					}
					if (!started)
					{
						reportManager.startTiming();
						started=true;
						logger.trace("Started processing");
	 				}
					currMsgCount.incrementAndGet();
					nProcessedThisThread++;
					
					recordLatency(msg);
					
					/**
					 * Post-process hook for overriding. Default is no-op.
					 */
					
//					statList.put(currMsgCount.get(),new StatRecord(msg, this.connection, this.sessionNum));
					
					postProcess(msg);

					// commit the transaction if necessary
					if ((txnSize > 0) && (currMsgCount.get() % txnSize == 0))
						txnHelper.commitTx(xaResource, session);

					/**
					 * If explicit client acknowledgement (or subclass
					 * overrides) are required
					 */

					if ((this.ackMode != Session.AUTO_ACKNOWLEDGE)
							&& (this.ackMode != Session.DUPS_OK_ACKNOWLEDGE)) {
						// Allows for provider-specific ack modes
						acknowledge(msg, this.ackMode);
					}									
				} // while loop
				//done.set(true);
				logger.debug("Message goal reached. This session (#"+sessionNum+") processed "+ nProcessedThisThread);
			} catch (JMSException e) {
				Exception le = e.getLinkedException();
				if (le==null || !(le instanceof InterruptedException)) {
					logger.error("Exception encountered in consumer thread", e);
				}
			} catch (OutOfMemoryError e) {
				long max = reportManager.getMaxMemory();
				long used = reportManager.getUsedMemory();
				logger.error("OutOfMemory error in consumer thread (" + used
						+ "M/" + max + "M)", e);
			} finally {
				logger.trace("Thread exiting after " + currMsgCount.get()
						+ " total messages received (" + nProcessedThisThread + " this thread)");
			}
		}
	}

	public static BidiMap<String, Integer> ackModes = new DualHashBidiMap<String, Integer>();

	// Class-specific fields/parameters
	private String selector = null;
	private Integer ackMode = null;
	private String subscriptionName = null;
	private String durableName = null;
	
	// variables
	private AtomicLong currMsgCount = new AtomicLong(0);
	private AtomicLong latencyTotal = new AtomicLong(0);
	private AtomicLong elapsed = new AtomicLong(0);
	private boolean done=false;
	private ReportManager reportManager;

	/**
	 * Constructor
	 * 
	 * @param inputConfig - A PropertiesConfiguration object describing the 
	 * default + command-line configuration
	 * @throws ConfigurationException
	 * @throws NamingException 
	 * @throws IllegalArgumentException 
	 */
	public JMSConsumer(PropertiesConfiguration inputConfig)
			throws IllegalArgumentException, NamingException {
		super(inputConfig);

		ackModes.put("AUTO_ACKNOWLEDGE", Session.AUTO_ACKNOWLEDGE);
		ackModes.put("CLIENT_ACKNOWLEDGE", Session.CLIENT_ACKNOWLEDGE);
		ackModes.put("DUPS_OK_ACKNOWLEDGE", Session.DUPS_OK_ACKNOWLEDGE);

		try {
		this.selector = config.getString(PROP_CONSUMER_SELECTOR);
		this.ackMode = ackModes.get(config
				.getString(PROP_CONSUMER_ACK_MODE));
		this.durableName = config.getString(PROP_CONSUMER_DURABLE);
		} catch (ConversionException ce)
		{
			throw new IllegalArgumentException(ce.getMessage(), ce);
		}
		printConsoleBanner();
	}

	public JMSConsumer(Builder builder) {
		super(builder);
	}

	protected synchronized void recordLatency(Message msg) throws JMSException
	{
		long msgTimestamp = msg.getJMSTimestamp();
		long currTime = System.currentTimeMillis();
		if (msgTimestamp !=0 )
		{
			this.latencyTotal.addAndGet(currTime-msgTimestamp);
		}
	}
	
	protected void acknowledge(Message msg, int mode) throws JMSException {
		msg.acknowledge();
	}

	protected void postProcess(Message msg) throws JMSException {
		// No-op. Override this to do provider-specific post processing
	
	}

	@Override
	public void startup() {
		try {
			createConnectionFactoryAndConnections();

			// create the reporting thread
			reportManager = new ReportManager(reportInterval, msgGoal);
			Thread reportingThread = new WorkerThread(reportManager, "ReportingThread");
			reportingThread.start();

			ThreadPoolExecutor pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(sessions);
			pool.setThreadFactory(new JMSWorkerThreadFactory("JMSConsumerThread"));
			pool.prestartAllCoreThreads();
			// create the consumer session threads

			/**
			 * TODO MUST make the SessionWorker derive its own destination name (again)
			 */

			JMSWorkerThread.setDebug(false);

			SessionWorker[] workers = new SessionWorker[this.sessions];
			for (int i=0; i < this.sessions; i++)
			{
				workers[i] = new SessionWorker(getConnection(), destName,
						this.ackMode, this.txnSize > 0, this.selector,
						this.subscriptionName, i);
				pool.execute(workers[i]);
				if ((i>=1000) && (i % 1000) == 0)
					logger.info(pool.getActiveCount() + " of " + this.sessions + " session threads created.");
			}
			while (pool.getActiveCount() < this.sessions)
			{
				logger.info("Waiting for all sessions to activate.");
				Thread.sleep(1000);
			}
			logger.info(pool.getActiveCount() + " of " + this.sessions + " session threads created.");
			if (duration > 0)
				logger.info("Waiting " + duration + " sec for sessions to complete.");
			else
				logger.info("Waiting for sessions to complete.");

			while ( (elapsed.get() < duration) && (currMsgCount.get() < msgGoal)) 
			{
				int sleepSeconds=1;
				Thread.sleep(sleepSeconds*1000);
				elapsed.addAndGet(sleepSeconds);
			}
			
			done=true;

			for (int i=0; i<this.sessions; i++)
			{
				logger.debug("Cleaning up session[" + i + "], which processed "
						+ workers[i].getNumProcessed() + " msgs");
				workers[i].cleanup();
			}
			
			logger.debug("Shutting down active threads.");
			logger.debug("poolSize=" + pool.getPoolSize());
			logger.debug("corePoolSize=" + pool.getCorePoolSize());
			logger.debug("maximumPoolSize=" + pool.getMaximumPoolSize());
			logger.debug("largestPoolSize=" + pool.getLargestPoolSize());
			logger.debug("taskCount=" + pool.getTaskCount());
			logger.debug("completedTasks=" + pool.getCompletedTaskCount());
			logger.debug("keepAliveTime=" + pool.getKeepAliveTime(TimeUnit.MILLISECONDS)+"msec");
			
//			pool.shutdownNow();

			logger.debug("Shutting down reporting thread");
			reportingThread.interrupt();

			// close connections
			logger.debug("Cleaning up connections");
			cleanupConnections();
			
		} catch (NamingException | JMSException e) {
			logger.error(e.getClass().getName() + " during setup:", e);
		} catch (OutOfMemoryError e) {
			long maxMemory = reportManager.getMaxMemory();
			long usedMemory = reportManager.getUsedMemory();
			logger.debug("\n\n(" + getNumConnections()
					+ " connections : Memory Use :" + usedMemory + "M/"
					+ maxMemory + "M)");
			logger.error("OutOfMemoryError during setup:", e);
		} catch (Exception e) {
			logger.error("Unexpected exception during setup:", e);
		}
		finally
		{
			logger.info("Done.");
		}
	}

	public class ReportManager implements Runnable {
		private MemoryUsage heapUsage = null;

		private long interval = 0;
		private long startTime = 0;
		private long stopTime = 0;
		private long lastSnapshotMsgCount = 0;
		private long msgLimit = 0;
		private boolean started = false;

		ReportManager(int howOften, long count) {
			this.interval = howOften;
			this.msgLimit = count;
		}

		protected synchronized void startTiming() {
			if (!started) {
				started=true;
				this.startTime = System.currentTimeMillis();
				logger.debug("Timing started");
			}
		}

		protected synchronized void stopTiming() {
			if (started)
			{
				started=false;
				this.stopTime=System.currentTimeMillis();
 				finalStats();
 				reportOverallPerformance();
			}
		}

		protected synchronized void finalStats() {
			// report final stats
			long elapsedSecs = (this.stopTime - this.startTime) / 1000;
			if (currMsgCount.get() > 0) {
				this.reportIntervalStatus(elapsedSecs);
			}
		}

		protected synchronized long getMaxMemory() {
			this.heapUsage = memoryBean.getHeapMemoryUsage();
			return (heapUsage.getMax() / MEGABYTE);
		}

		protected synchronized long getUsedMemory() {
			this.heapUsage = memoryBean.getHeapMemoryUsage();
			return (heapUsage.getUsed() / MEGABYTE);
		}

		protected synchronized void reportIntervalStatus(long elapsed) {
			long msgsThisInterval = currMsgCount.get() - lastSnapshotMsgCount;
			long totalElapsed = System.currentTimeMillis() - this.startTime; 
			double averageLatency = (latencyTotal.get()/currMsgCount.get());
			if ((msgsThisInterval > 0) && (elapsed > 0)) {
				long currMsgRate = (long) (msgsThisInterval / elapsed);
				long msgs = currMsgCount.get();
				long max = msgLimit;
				long cumulative = msgs/(totalElapsed / 1000);
				logger.info(String.format("%8d", msgs) 
						+ "/" 
						+ String.format("%8d",max)
						+ " msgs rcvd (" + currMsgRate + " msg/sec), " 
						+ cumulative + " msg/sec cumulative. latency="
						+ String.format("%3.2f", averageLatency) + "ms "
						+ getUsedMemory() + "MB/" + getMaxMemory() + "MB");
				lastSnapshotMsgCount = currMsgCount.get();
			} else {
				logger.debug("Reporting interval elapsed but nothing to report");
			}
		}

		/**
		 * Get the performance results.
		 */
		protected synchronized void reportOverallPerformance() {
			long elapsed = this.stopTime - this.startTime;
			if (elapsed > 0) {
				double seconds = elapsed / 1000.0;
				int perf = (int) ((currMsgCount.get() * 1000.0) / elapsed);
				logger.info(currMsgCount + " messages took " + seconds
						+ " seconds, performance is " + perf + " msg/sec "
						+ "(" + perf / sessions + " msg/sec/session)");
			} else {
				logger.info("interval too short to calculate a message rate");
			}
		}

		public void run() {
			logger.trace("Reporting manager started.");
			while (currMsgCount.get() == 0) {

			}
			long start = System.currentTimeMillis();
			while (!done) {
				long current=System.currentTimeMillis();
				long elapsed=(current-start)/1000;
				if (elapsed==this.interval)
				{
					this.reportIntervalStatus(elapsed);
					start=current;
				}
			}
			this.stopTiming();
		}
	}

	@Override
	public synchronized void pause() {
		// TODO Auto-generated method stub

	}

	@Override
	public void pause(int msec) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub

	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public void end() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPaused() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

	public void printConsoleBanner()
	{
		// print parameters
		System.err.println();
		System.err
				.println("------------------------------------------------------------------------");
		String className = this.getClass().getName();
		className = className.substring((className.lastIndexOf('.') + 1));
		System.err.println(className);
		System.err
				.println("------------------------------------------------------------------------");
		if (flavor != null)
			System.err.println("Broker Flavor................ " + flavor);
		System.err.println("Broker URL................... " + getBrokerURL());
		System.err.println("Initial Context Factory...... "
				+ contextFactoryName);
		System.err.println("Connection Factory........... "
				+ connectionFactoryName);
		System.err.println("User......................... " + username);
		System.err.println("Destination.................. " + "(" + destType
				+ ") " + destName);
		if (durableName != null)
			System.err.println("Durable...................... "
					+ (durableName != null));
		System.err.println("Consumer Connections......... " + connections);
		System.err.println("Consumer Sessions............ " + sessions);
		System.err.println("Unique Destinations.......... " + uniqueDests);
		System.err.println("Count........................ " + msgGoal);
		System.err.println("Duration..................... " + duration);
		System.err.println("Ack Mode..................... "
				+ ackModes.inverseBidiMap().get(this.ackMode));
		if (selector != null)
			System.err.println("Selector..................... " + selector);
		System.err.println("XA........................... " + xa);
		if (getTxnSize() > 0)
			System.err.println("Transaction Size............. " + getTxnSize());
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println();
	}
	
	public static class Builder extends JMSClient.ClientBuilder
	{
		private String selector;
		private String durable;
		private String ackModeString;

		public Builder selector(String selector)
		{
			this.selector = selector;	return this;
		}
		
		public Builder durable(String durable)
		{
			this.durable = durable;		return this;
		}
		
		public Builder ackMode(String ackMode)
		{
			this.ackModeString=ackMode;	return this;
		}
		
		public JMSConsumer build()
		{
			return new JMSConsumer(this);
		}
	}
}
