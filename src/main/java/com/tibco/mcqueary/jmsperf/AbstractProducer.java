package com.tibco.mcqueary.jmsperf;

/* 
 * Copyright (c) 2001-$Date: 2009-09-11 14:08:04 -0700 (Fri, 11 Sep 2009) $ TIBCO Software Inc. 
 * All rights reserved.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: jmsMsgProducerPerf.java 41997 2009-09-11 21:08:04Z bmahurka $
 * 
 */

/*
 * This is a sample message producer class used to measure performance.
 *
 * For the the specified number of sessions this sample creates a 
 * session and a message producer for the specified destination.
 * Once the specified number of messages are produced the performance
 * results are printed and the program exits.
 *
 * Usage:  java jmsMsgProducerPerf  [options]
 *
 *  where options are:
 *
 *   -jndi         <url>         JNDI Provider URL. Default is null.
 *   -user         <username>    User name. Default is null.
 *   -password     <password>    User password. Default is null.
 *   -topic        <topic-name>  Topic name. Default is "topic.sample".
 *   -queue        <queue-name>  Queue name. No default.
 *   -size         <num bytes>   Message payload size in bytes. Default is 100.
 *   -msgGoal        <num msgs>    Number of messages to send. Default is 10k.
 *   -time         <seconds>     Number of seconds to run. Default is 0 (forever).
 *   -delivery     <mode>        Delivery mode. Default is NON_PERSISTENT.
 *                               Other values: PERSISTENT and RELIABLE.
 *   -sessions      <num sessions> Number of message producer sessions. Default is 1.
 *   -connections  <num conns>   Number of message producer connections. Default is 1.
 *   -txnsize      <num msgs>    Number of nessages per producer transaction. Default is 0.
 *   -rate         <msg/sec>     Message rate for producer sessions.
 *   -payload      <file name>   File containing message payload.
 *   -factory      <lookup name> Lookup name for connection factory.
 *   -uniquedests                Each producer thread uses a unique destination.
 *   -compression                Enable message compression.
 *   -xa                         Use XA transactions.
 */

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;

import static com.tibco.mcqueary.jmsperf.Constants.*;
import static com.tibco.mcqueary.jmsperf.Constants.DeliveryMode;

public abstract class AbstractProducer extends Client implements Runnable {

	// fields
	protected DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;
	protected int msgRate = PROP_PRODUCER_RATE_DEFAULT;
	protected boolean compression = PROP_PRODUCER_COMPRESSION_DEFAULT;
	protected boolean timestampEnabled = PROP_PRODUCER_TIMESTAMP_DEFAULT;
	protected boolean useMessageID = PROP_PRODUCER_MESSAGEID_DEFAULT;

	// abstract message sender fields
	protected int payloadSize = PROP_PRODUCER_PAYLOAD_SIZE_DEFAULT;
	protected int payloadMinSize = PROP_PRODUCER_PAYLOAD_MINSIZE_DEFAULT;
	protected int payloadMaxSize = PROP_PRODUCER_PAYLOAD_MAXSIZE_DEFAULT;
	protected byte[] payloadBytes = null;
	protected String payloadFileName = PROP_PRODUCER_PAYLOAD_FILENAME_DEFAULT;
	protected boolean useRandomSize = false;

	// static variables
	protected static Random randomizer = new Random();

	// instance variables
	protected long startTime;
	protected long endTime;
	protected long elapsed;
	protected boolean stopNow;

	protected ReportManager reportManager;
	private AtomicBoolean done = new AtomicBoolean(false);

	// create the producer threads
	/**
	 * Constructor
	 * 
	 * @param args
	 *            the command line arguments
	 * @throws ConfigurationException
	 * @throws NamingException
	 * @throws IllegalArgumentException
	 */
	public AbstractProducer(PropertiesConfiguration inputConfig)
			throws IllegalArgumentException, NamingException {
		super(inputConfig);

		logger.debug(logger.getName());
		logger.debug(ConfigHandler.getConfigString("AbstractProducer constructor",
				this.config));

		try {
			payloadFileName = config.getString(
					PROP_PRODUCER_PAYLOAD_FILENAME, payloadFileName);
			compression = config.getBoolean(
					PROP_PRODUCER_COMPRESSION, compression);
			msgRate = config.getInt(PROP_PRODUCER_RATE, 0);
			String payloadSizeString = config.getString(
					PROP_PRODUCER_PAYLOAD_SIZE, null);
			if (payloadSizeString != null)
				payloadSize = ConfigHandler.toBytes(payloadSizeString);
			payloadMinSize = config.getInt(
					PROP_PRODUCER_PAYLOAD_MINSIZE, payloadMinSize);
			payloadMaxSize = config.getInt(
					PROP_PRODUCER_PAYLOAD_MAXSIZE, payloadMaxSize);
			if (payloadMinSize < payloadMaxSize)
				useRandomSize = true;
			String deliveryModeString = config.getString(
					PROP_PRODUCER_DELIVERY_MODE).toUpperCase();
			deliveryMode = DeliveryMode.valueOf(deliveryModeString);
			timestampEnabled = config.getBoolean(
					PROP_PRODUCER_TIMESTAMP, timestampEnabled);
			useMessageID = config.getBoolean(PROP_PRODUCER_MESSAGEID, useMessageID);
		} catch (ConversionException ce) {
			throw new IllegalArgumentException(ce.getMessage(), ce);
		}
	}

	protected AbstractProducer(Builder builder) {
		super(builder);
		this.payloadFileName = builder.payloadFileName;
		this.compression = builder.compression;
		this.msgRate = builder.msgRate;
		this.payloadSize = builder.payloadSize;
		this.payloadMinSize = builder.payloadMinSize;
		this.payloadMaxSize = builder.payloadMaxSize;
		this.useRandomSize = builder.useRandomSize;
		this.deliveryMode = builder.deliveryMode;
		this.timestampEnabled = builder.timestampEnabled;
	}

	@Override
	public void setup() {

		try {
			if (!isXa())
				createConnections();
			else
				createXAConnectionFactoryAndXAConnections();

			reportManager = new ReportManager(reportInterval, 
					msgGoal, sessions, warmup, offset);
			Thread reportingThread = new WorkerThread(reportManager, "ReportingThread");
			reportingThread.start();
			
			// create the producer threads
			Vector<Thread> tv = new Vector<Thread>(sessions);
			for (int i = 0; i < sessions; i++) {
				Thread t = new Thread(this);
				tv.add(t);
				t.start();
				if (tv.size() > 1 && (i % 1000) == 0)
					logger.info(tv.size() + " of " + sessions
							+ " sessions created.");

			}
			logger.info(tv.size() + " of " + sessions + " sessions created.");

			logger.info(shortClassName + " commenced processing.");

			// run for the specified amount of time
			if (getDuration() > 0) {
				try {
					Thread.sleep(getDuration() * 1000);
				} catch (InterruptedException e) {
				}

				// ensure producer threads stop now
				stopNow = true;
				for (int i = 0; i < sessions; i++) {
					Thread t = (Thread) tv.elementAt(i);
					t.interrupt();
				}
			}

			// wait for the producer threads to exit
			for (int i = 0; i < sessions; i++) {
				Thread t = (Thread) tv.elementAt(i);
				try {
					t.join();
				} catch (InterruptedException e) {
				}
			}

			logger.debug("Shutting down reporting thread");
			reportingThread.interrupt();
			
			// close connections
			cleanupConnections();

		} catch (NamingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public static int getRandomInt(int Low, int High) {
		int R = randomizer.nextInt(High - Low) + Low;
		return R;
	}

	/**
	 * The producer thread's run method.
	 */
	public void run() {
		boolean started = false;
		MsgRateChecker msgRateChecker = null;

		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}

		try {
			MessageProducer msgProducer = null;
			Destination destination = null;
			Session session = null;
			XAResource xaResource = null;
			JMSPerfTxnHelper txnHelper = getPerfTxnHelper(xa);
			
			if (!xa) {
				// get the connection
				Connection connection = getConnection();
				// create a session
				session = connection.createSession(txnSize > 0,
						Session.AUTO_ACKNOWLEDGE);
			} else {
				// get the connection
				XAConnection connection = getXAConnection();
				// create a session
				session = connection.createXASession();
			}

			if (xa)
				/* get the XAResource for the XASession */
				xaResource = ((javax.jms.XASession) session).getXAResource();

			// get the destination
			destination = getDestination(session);

			// create the producer
			msgProducer = session.createProducer(destination);
			
			// set the delivery mode
			msgProducer.setDeliveryMode(deliveryMode.value());

			// performance settings
			msgProducer.setDisableMessageID(!this.isMessageIDEnabled());
			msgProducer.setDisableMessageTimestamp(!this.isTimestampEnabled());

			// create the message
			BytesMessage msg = session.createBytesMessage();
			if (this.payloadSize > 0) {
				this.payloadBytes = createPayload(this.payloadFileName,
						this.payloadSize);
				msg.writeBytes(this.payloadBytes);
			}

			// enable compression if necessary
			if (compression && payloadSize > 0)
				compressMessageBody(msg);

			// initialize message rate checking
			if (msgRate > 0)
				msgRateChecker = new MsgRateChecker(msgRate);

//			startTiming();
//			reportManager.startTiming();
			
			int randomSize = 0;

			// publish messages
			while ((msgGoal == 0 || reportManager.getMsgCount() < (msgGoal / sessions))
					&& !stopNow) {
				// a no-op for local txns
				txnHelper.beginTx(xaResource);

//				payloadBytes = this.createPayload(payloadFileName, randomSize);
				msg.clearBody();

				if (useRandomSize) {

					randomSize = getRandomInt(payloadMinSize, payloadMaxSize);

					// logger.trace("Generating message body of size " +
					// randomSize + " bytes.");
					msg.clearBody();
					try {
						((BytesMessage) msg).writeBytes(payloadBytes, 0,
								randomSize);
					} catch (IndexOutOfBoundsException e) {
						logger.error(
								"Caught IndexOutOfBoundsException while writng message body:"
										+ "	payloadBytes.length == "
										+ payloadBytes.length
										+ "	randomSize == " + randomSize, e);
					}
				}

				if (reportManager.getMsgCount() < msgGoal) {
					if (!started)
					{
						reportManager.startTiming();
						started=true;
						logger.trace("Started processing");
	 				}

					preProcess(msg);
					msgProducer.send(msg);
					postProcess(msg);
					reportManager.incrementMsgCount();
				}

				// commit the transaction if necessary
				if (txnSize > 0 && reportManager.getMsgCount() % txnSize == 0)
					txnHelper.commitTx(xaResource, session);

				// check the message rate
				if (msgRate > 0)
					msgRateChecker.throttleMsgRate(reportManager.getMsgCount());
			}

			// commit any remaining messages
			if (txnSize > 0)
				txnHelper.commitTx(xaResource, session);
		} catch (JMSException e) {
			if (!stopNow) {
				logger.error("JMSException: ", e);
				Exception le = e.getLinkedException();
				if (le != null) {
					logger.error("Linked exception: ", le);
				}
			}
		}

		reportManager.stopTiming();

	}

	protected synchronized void postProcess(BytesMessage msg) {
		// TODO Auto-generated method stub
		
	}

	protected synchronized void preProcess(BytesMessage msg) {
		// TODO Auto-generated method stub
		
	}

	protected boolean isMessageIDEnabled() {
		// TODO Auto-generated method stub
		return useMessageID;
	}

	/**
	 * Compresses the Message body, if supported by the provider. Default
	 * implementation is a no-op
	 * 
	 * @param msg
	 *            - The message to be compressed
	 * @return success/failure
	 * @throws JMSException
	 */
	public void compressMessageBody(Message msg) throws JMSException {
		// No-op for generic AbstractProducer
	}

//	protected synchronized void startTiming() {
//		if (startTime == 0)
//			startTime = System.currentTimeMillis();
//	}

//	protected synchronized void stopTiming() {
//		endTime = System.currentTimeMillis();
//	}

	/**
	 * Class used to control the producer's send rate.
	 */

	class MsgRateChecker {
		private long sampleStart;
		private int sampleTime;
		private long sampleCount;
		private int rate;

		MsgRateChecker(int rate) {
			this.rate = rate;
			// initialize
			this.sampleTime = 10;
		}

		synchronized void throttleMsgRate(long count) {
			if (this.rate < 100) {
				if (count % 10 == 0) {
					try {
						long sleepTime = (long) ((10.0 / (double) this.rate) * 1000);
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
					}
				}
			} else if (sampleStart == 0) {
				sampleStart = System.currentTimeMillis();
			} else {
				long elapsed = System.currentTimeMillis() - sampleStart;
				if (elapsed >= sampleTime) {
					long actualMsgs = count - sampleCount;
					long expectedMsgs = (int) (elapsed * ((double) this.rate / 1000.0));
					if (actualMsgs > expectedMsgs) {
						long sleepTime = (long) ((double) (actualMsgs - expectedMsgs) / ((double) this.rate / 1000.0));
						try {
							// long threadId = Thread.currentThread().getId();
							// System.err.println(threadId + ": Sleeping " +
							// sleepTime + "msec to slow down message rate");
							Thread.sleep(sleepTime);
						} catch (InterruptedException e) {
						}
						if (sampleTime > 20)
							sampleTime -= 10;
					} else {
						if (sampleTime < 300)
							sampleTime += 10;
					}
					sampleStart = System.currentTimeMillis();
					sampleCount = count;
				}
			}
		}
	}

	public void printConsoleBanner() {

		printConsoleBannerHeader();
		printCommonSettings();

		//can be overridden by subclass to print other subclass-specific settings
		printSpecificSettings();

		printConsoleBannerFooter();
	}
	
	public void printSpecificSettings()
	{
		System.err.println("Timestamp Messages........... "
				+ this.isTimestampEnabled());
		if (useRandomSize) {
			System.err.println("Min Message Size............. "
					+ payloadMinSize);
			System.err.println("Max Message Size............. "
					+ (payloadFileName != null ? payloadFileName : String
							.valueOf(payloadMaxSize)));
		} else if (payloadFileName != null) {
			System.err.println("Message Size................. "
					+ (payloadFileName != null ? payloadFileName : String
							.valueOf(payloadMaxSize)));
		} else {
			System.err.println("Message Size................. " + payloadSize);
		}
		System.err.println("DeliveryMode................. " + deliveryMode);
		System.err.println("Compression.................. " + compression);

	}

	/**
	 * @return the timestampEnabled
	 */
	protected synchronized boolean isTimestampEnabled() {
		return timestampEnabled;
	}

	/**
	 * @param timestampEnabled
	 *            the timestampEnabled to set
	 */
	protected synchronized void setTimestampEnabled(boolean timestampEnabled) {
		this.timestampEnabled = timestampEnabled;
	}

	public static abstract class Builder<T extends Client.Builder<T>> extends Client.Builder<T>
	{
//	public static abstract class Builder extends Client.Builder<Builder> implements
//			com.tibco.mcqueary.jmsperf.Builder {

		private DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;
		private int msgRate = PROP_PRODUCER_RATE_DEFAULT;
		private boolean compression = PROP_PRODUCER_COMPRESSION_DEFAULT;

		private boolean timestampEnabled = PROP_PRODUCER_TIMESTAMP_DEFAULT;

		// abstract message sender fields
		protected int payloadSize = PROP_PRODUCER_PAYLOAD_SIZE_DEFAULT;
		protected int payloadMinSize = PROP_PRODUCER_PAYLOAD_MINSIZE_DEFAULT;
		protected int payloadMaxSize = PROP_PRODUCER_PAYLOAD_MAXSIZE_DEFAULT;
		protected byte[] payloadBytes = null;
		protected String payloadFileName = PROP_PRODUCER_PAYLOAD_FILENAME_DEFAULT;
		protected boolean useRandomSize = false;

		@Override
		protected abstract T me();

		public T delivery(DeliveryMode mode) {
			this.deliveryMode = mode;
			return me();
		}

		public T rate(int rate) {
			this.msgRate = rate;
			return me();
		}

		public T compression(boolean compress) {
			this.compression = compress;
			return me();
		}

		public T timestamp(boolean timestamp) {
			this.timestampEnabled = timestamp;
			return me();
		}

		public T payloadSize(int size) {
			this.payloadSize = size;
			return me();
		}

		public T payloadMinSize(int min) {
			this.payloadMinSize = min;
			return me();
		}

		public T payloadMaxSize(int max) {
			this.payloadMaxSize = max;
			return me();
		}

		public T payloadBytes(byte[] payloadBytes) {
			this.payloadBytes = payloadBytes;
			return me();
		}

		public T payloadFileName(String name) {
			this.payloadFileName = name;
			return me();
		}

		public T randomPayloadSize(boolean random) {
			this.useRandomSize = random;
			return me();
		}

	}

}
