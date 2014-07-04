package com.tibco.mcqueary.jmsperf;

import java.lang.management.MemoryUsage;
import java.util.Vector;

import javax.jms.*;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;

public class JMSConsumer extends JMSWorker
implements Runnable
{
	public static final String PROPERTY_FILE = "consumer.properties";

	public static BidiMap<String,Integer> ackModes = 
            new DualHashBidiMap<String, Integer>();

	static {
	    
    }

	// Class-specific fields/parameters
	private String selector = null;
	private Integer ackMode = null;
	private String subscriptionName = null;

	// variables
	private boolean stopNow = false;
	private int nameIter = 0;

	private ReportManager reportManager;

	/**
	 * Constructor
	 * 
	 * @param args
	 *            the command line arguments
	 */
	
	public JMSConsumer(Configuration inputConfig) {
		super(inputConfig);
		
	    ackModes.put("AUTO_ACKNOWLEDGE", Session.AUTO_ACKNOWLEDGE);
    	ackModes.put("CLIENT_ACKNOWLEDGE", Session.CLIENT_ACKNOWLEDGE);
    	ackModes.put("DUPS_OK_ACKNOWLEDGE", Session.DUPS_OK_ACKNOWLEDGE);
		LOGGER.debug("In initParams() for " + this.getClass().getName());
		try {
		selector = config.getString(Manager.PROP_CONSUMER_SELECTOR);
		ackMode = ackModes.get(config.getString(Manager.PROP_CONSUMER_ACK_MODE));
		durableName = config.getString(Manager.PROP_CONSUMER_DURABLE);
		} catch (ConversionException e)
		{
			LOGGER.error(e.getMessage());
		}
		// print parameters
		System.err.println();
		System.err
				.println("------------------------------------------------------------------------");
		String className = this.getClass().getName();
		className = className.substring((className.lastIndexOf('.')+1));
		System.err.println(className);
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println("Broker Flavor................ " + flavor);
		System.err.println("JNDI Provider................ " + jndiProviderURL);
		System.err.println("Initial Context Factory...... " + contextFactoryName);
		System.err.println("Connection Factory........... " + connectionFactoryName);
		System.err.println("User......................... " + username);
		System.err.println("Destination.................. " + "(" + destType
				+ ") " + destName);
		if (durableName != null)
			System.err.println("Durable...................... "
					+ (durableName != null));
		System.err.println("Unique Destinations.......... " + uniqueDests);
		System.err.println("Count........................ " + count);
		System.err.println("Duration..................... " + runTime);
		System.err.println("Consumer Sessions............ " + sessions);
		System.err.println("Consumer Connections......... " + connections);
		System.err.println("Ack Mode..................... " + ackModes.inverseBidiMap().get(ackMode));
		System.err.println("Selector..................... " + selector);
		System.err.println("XA........................... " + xa);
		if (txnSize != null)
			System.err.println("Transaction Size............. " + txnSize);
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println();
	}


	public void start() {
		try {
			// CUT FROM HERE
			if (!xa)
				createConnectionFactoryAndConnections();
			else
				createXAConnectionFactoryAndXAConnections();

			if (connsVector.size() < connections) {
				LOGGER.error("Couldn't allocate requested connections. Exiting.");
				System.exit(1);
			}

			// create the reporting thread
			reportManager = new ReportManager(reportInterval);
			Thread reportingThread = new Thread(reportManager);
			reportingThread.start();

			// create the consumer session threads
			Vector<Thread> tv = new Vector<Thread>(sessions);
			for (int i = 0; i < sessions; i++) {
				Thread t = new Thread(this);
				tv.add(t);
				t.start();
				System.err.print(String.format("\r    Threads created: %6d",
						tv.size()));
			}

			System.err.println();
			LOGGER.info("READY");

			// run for the specified amount of time
			if (runTime > 0) {
				try {
					Thread.sleep(runTime * 1000);
				} catch (InterruptedException e) {
				}

				// ensure consumer threads stop now
				stopNow = true;
				for (int i = 0; i < sessions; i++) {
					Thread t = (Thread) tv.elementAt(i);
					t.interrupt();
				}
			}

			// wait for the consumer threads to exit
			for (int i = 0; i < sessions; i++) {
				Thread t = (Thread) tv.elementAt(i);
				try {
					t.join();
				} catch (InterruptedException e) {
				}
			}

			// close connections
			cleanup();

			// stop reporting thread
			reportManager.shutdown();

			// print performance
			reportManager.reportOverallPerformance();
		} catch (NamingException | JMSException e) {
			LOGGER.error(e.getClass().getName() + " during setup:", e);
		} catch (OutOfMemoryError e) {
			// long maxMemory = reportManager.getMaxMemory();
			// long usedMemory = reportManager.getUsedMemory();
			// LOGGER.debug("\n\n("+connsVector.size() +
			// " connections : Memory Use :" + usedMemory
			// + "M/" + maxMemory + "M)");
			LOGGER.error("OutOfMemoryError during setup:", e);
		} catch (Exception e) {
			LOGGER.error("Unexpected exception during setup:", e);
		}
	}

	/**
	 * The consumer thread's run method.
	 */
	public void run() {
		Session session = null;
		MessageConsumer msgConsumer = null;
		// int msgCount = 0;
		Destination destination = null;
		XAResource xaResource = null;
		JMSPerfTxnHelper txnHelper = getPerfTxnHelper(xa);

		try {
			Thread.sleep(250);
		} catch (InterruptedException e) {
		}

		try {
			LOGGER.trace("Running thread");

			if (!xa) {
				// get the connection
				Connection connection = getConnection();
				// create a session
				session = connection.createSession(txnSize > 0, ackMode);
			} else {
				// get the connection
				XAConnection connection = getXAConnection();
				// create a session
				session = connection.createXASession();
			}
			if (session == null)
				return;

			if (xa)
				/* get the XAResource for the XASession */
				xaResource = ((javax.jms.XASession) session).getXAResource();

			// get the destination
			destination = getDestination(session);

			// create the consumer
			if (subscriptionName == null)
				msgConsumer = session.createConsumer(destination, selector);
			else
				msgConsumer = session.createDurableSubscriber(
						(Topic) destination, subscriptionName, selector, false);

			LOGGER.trace("Subscribed to " + destination);

			// register multicast exception listener for multicast consumers
			// if (com.tibco.tibjms.Tibjms.isConsumerMulticast(msgConsumer))
			// com.tibco.tibjms.Tibjms.setMulticastExceptionListener(this);

			// boolean startNewXATxn = true;

			// receive messages
			while ((count == 0 || reportManager.getMsgCount() < (count / sessions))
					&& !stopNow) {
				LOGGER.trace("In receive loop");

				// a no-op for local txns
				txnHelper.beginTx(xaResource);

				Message msg = msgConsumer.receive();
				LOGGER.trace("Got a message");

				if (msg == null)
					break;

				if (reportManager.getMsgCount() == 0)
					reportManager.startTiming();

				reportManager.incrementMsgCount();

				// acknowledge the message if necessary
				
				if ((ackMode !=  Session.AUTO_ACKNOWLEDGE) &&
						(ackMode != Session.DUPS_OK_ACKNOWLEDGE))
				{
					//  Allows for provider-specific override
					acknowledge(msg, ackMode);
				}


				// commit the transaction if necessary
				if (txnSize > 0 && reportManager.getMsgCount() % txnSize == 0)
					txnHelper.commitTx(xaResource, session);

				if (flavor == Flavor.TIBEMS) {
					// force the uncompression of compressed messages
					if (msg.getBooleanProperty("JMS_TIBCO_COMPRESS"))
						((BytesMessage) msg).getBodyLength();
				}
			}
		} catch (JMSException e) {
			if (!stopNow) {
				LOGGER.error("Exception encountered in consumer thread", e);

				Exception le = e.getLinkedException();
				if (le != null) {
					LOGGER.error("linked exception: ", le);
				}
			}
		} catch (OutOfMemoryError e) {
			long max = reportManager.getMaxMemory();
			long used = reportManager.getUsedMemory();
			LOGGER.error("OutOfMemory error in consumer thread (" + used + "M/"
					+ max + "M)", e);
		}

		// commit any remaining messages
		if (txnSize > 0) {
			try {
				txnHelper.commitTx(xaResource, session);
			} catch (JMSException e) {
				if (!stopNow)
					LOGGER.error("JMSException", e);
			}
		}

		reportManager.stopTiming();

		reportManager.finalStats();

		try {
			if (msgConsumer != null)
				msgConsumer.close();

			// unsubscribe durable subscription
			if (subscriptionName != null) {
				if (session != null)
					session.unsubscribe(subscriptionName);
			}
			if (session != null)
				session.close();
		} catch (JMSException e) {
			LOGGER.error("JMSException", e);
		}
	}

	protected void acknowledge(Message msg, int mode) throws JMSException {
		// TODO Auto-generated method stub
			msg.acknowledge();
	}

	/**
	 * Returns a unique subscription name if durable subscriptions are
	 * specified, synchronized because of multiple prod/cons threads
	 */
	protected synchronized String getSubscriptionName() {
		if (durableName != null)
			return durableName + ++nameIter;
		else
			return null;
	}

	public class ReportManager implements Runnable {
		private MemoryUsage heapUsage = null;

		private int interval;
		private volatile long startTime;
		private volatile long stopTime;
		private volatile long lastSnapshotMsgCount = 0;
		private volatile long currMsgCount = 0;

		private boolean done = false;

		ReportManager(int howOften) {
			this.interval = howOften;
		}

		protected synchronized void startTiming() {
			if (startTime == 0)
				startTime = System.currentTimeMillis();
		}

		protected synchronized void stopTiming() {
			this.stopTime = System.currentTimeMillis();
		}

		protected synchronized long getStartTime() {
			return this.startTime;
		}

		protected synchronized long getStopTime() {
			return this.stopTime;
		}

		protected synchronized long incrementMsgCount() {
			this.currMsgCount++;
			return this.currMsgCount;
		}

		/**
		 * Get the total consumed message count.
		 */
		protected synchronized long getMsgCount() {
			return this.currMsgCount;
		}

		protected synchronized void finalStats() {
			long elapsedSecs;
			// report final stats
			elapsedSecs = (this.stopTime - this.startTime) / 1000;
			if (this.currMsgCount > 0) {
				LOGGER.info("TEST COMPLETE. Final Results:");
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
			long msgsThisInterval = currMsgCount - lastSnapshotMsgCount;
			int currMsgRate;
			if (msgsThisInterval > 0) {
				currMsgRate = (int) (msgsThisInterval / elapsed);
				LOGGER.info(currMsgCount + " msgs received(total). Rate for "
						+ elapsed + " seconds is " + currMsgRate + " msg/sec");
				lastSnapshotMsgCount = currMsgCount;
			}
		}

		/**
		 * Get the performance results.
		 */
		protected synchronized void reportOverallPerformance() {
			long elapsed = this.getStopTime() - this.getStartTime();
			if (elapsed > 0) {
				double seconds = elapsed / 1000.0;
				int perf = (int) ((this.currMsgCount * 1000.0) / elapsed);
				LOGGER.info(this.currMsgCount + " messages took " + seconds
						+ " seconds, performance is " + perf + " msg/sec "
						+ "(" + perf / sessions + " msg/sec per thread)");
			} else {
				LOGGER.info("interval too short to calculate a message rate");
			}
		}

		public void run() {
			LOGGER.trace("Reporting manager started.");
			while (!done) {
				try {
					Thread.sleep(this.interval * 1000);
					this.reportIntervalStatus(this.interval);
				} catch (InterruptedException e) {
				}
			}
		}

		public void shutdown() {
			done = true;
		}
	}

	@Override
	public void pause() {
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

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(int seconds) {
		// TODO Auto-generated method stub
		
	}
}
