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

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;

import static com.tibco.mcqueary.jmsperf.Executive.PFX;

public class JMSProducer extends JMSClient implements Runnable {

	public static final String PROP_PRODUCER_RATE = PFX + "producer.send.rate";
	public static final String PROP_PRODUCER_COMPRESSION = PFX
			+ "producer.message.compression";
	public static final String PROP_PRODUCER_PAYLOAD_FILENAME = PFX
			+ "producer.payload.filename";
	public static final String PROP_PRODUCER_PAYLOAD_SIZE = PFX
			+ "producer.payload.size";
	public static final String PROP_PRODUCER_PAYLOAD_MINSIZE = PFX
			+ "producer.message.size.minimum";
	public static final String PROP_PRODUCER_PAYLOAD_MAXSIZE = PFX
			+ "producer.message.size.maximum";
	public static final String PROP_PRODUCER_DELIVERY_MODE = PFX
			+ "producer.delivery.mode";
	public static final String PROP_PRODUCER_TIMESTAMP = PFX
			+ "producer.message.timestamp";

	public static BidiMap<String,Integer> deliveryModes = 
            new DualHashBidiMap<String, Integer>();
	
	// parameters
	private Integer deliveryMode = null;
	private Integer msgRate = null;
	private boolean compression = false;
	private Integer payloadMinBytes = null;
	private Integer payloadMaxBytes = null;
	private byte[] payloadBytes = null;
	private String payloadFile = null;
	private boolean useRandomSize = false;
	private boolean timestampEnabled =  false;

	// variables
	private long startTime;
	private long endTime;
	private long elapsed;
	private boolean stopNow;
	private StringBuffer msgBuffer = null;

	private Random rand = null;
	private volatile int currMsgCount = 0;
	private volatile int sentCount = 0;

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
	public JMSProducer(PropertiesConfiguration inputConfig) 
					throws IllegalArgumentException, NamingException {
		super(inputConfig);

		logger.debug(logger.getName());
		logger.debug(ConfigHandler.getConfigString("JMSProducer constructor", this.config));
		
		deliveryModes.put("NON_PERSISTENT", DeliveryMode.NON_PERSISTENT);
		deliveryModes.put("PERSISTENT", DeliveryMode.PERSISTENT);
	
		try {
			payloadFile = config.getString(PROP_PRODUCER_PAYLOAD_FILENAME);
			compression = config.getBoolean(PROP_PRODUCER_COMPRESSION, false);
			msgRate = config.getInt(PROP_PRODUCER_RATE,0);
			payloadMinBytes = config.getInt(PROP_PRODUCER_PAYLOAD_MINSIZE,0);
			payloadMaxBytes = config.getInt(PROP_PRODUCER_PAYLOAD_MAXSIZE,0);
			if (payloadMinBytes < payloadMaxBytes)
				useRandomSize = true;
			deliveryMode = deliveryModes.get(config.getString(PROP_PRODUCER_DELIVERY_MODE));
			timestampEnabled = config.getBoolean(PROP_PRODUCER_TIMESTAMP);
			
		} catch (ConversionException ce)
		{
			throw new IllegalArgumentException(ce.getMessage(), ce);
		}
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startup() {
		try {
			printConsoleBanner();

			if (!isXa())
				createConnectionFactoryAndConnections();
			else
				createXAConnectionFactoryAndXAConnections();

			// create the producer threads
			Vector<Thread> tv = new Vector<Thread>(sessions);
			for (int i = 0; i < sessions; i++) {
				Thread t = new Thread(this);
				tv.add(t);
				t.start();
				if (tv.size() > 1 && (i%1000)==0)
					logger.info(tv.size() + " of " + sessions + " sessions created.");
					
			}
			logger.info(tv.size() + " of " + sessions
					+ " sessions created.");
			
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

			// close connections
			cleanupConnections();

			// print performance
			logger.info(getPerformance());
		} catch (NamingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Update the total sent msgGoal.
	 */
	private synchronized void countSends(int count) {
		sentCount += count;
	}

	/**
	 * The producer thread's run method.
	 */
	public void run() {
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
			msgProducer.setDeliveryMode(deliveryMode);

			// performance settings
			msgProducer.setDisableMessageID(true);
			msgProducer.setDisableMessageTimestamp(!this.isTimestampEnabled());

			// create the message
			Message msg = createMessage(session);

			// enable compression if necessary
			if (compression)
				compressMessageBody(msg); 
				
			// initialize message rate checking
			if (msgRate > 0)
				msgRateChecker = new MsgRateChecker(msgRate);

			startTiming();

			int randomSize = 0;

			// publish messages
			while ((msgGoal == 0 || getMsgCount() < (msgGoal / sessions)) && !stopNow) {
				// a no-op for local txns
				txnHelper.beginTx(xaResource);

				if (useRandomSize) {
					if (rand == null)
						rand = new Random();
					randomSize = rand.nextInt(payloadMaxBytes - payloadMinBytes + 1)
							+ payloadMinBytes;
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
				synchronized(this)
				{
					if (getMsgCount() < msgGoal)
					{
						msgProducer.send(msg);
						bumpMsgCount();
					}
				}
				
				// commit the transaction if necessary
				if (txnSize > 0 && getMsgCount() % txnSize == 0)
					txnHelper.commitTx(xaResource, session);

				// check the message rate
				if (msgRate > 0)
					msgRateChecker.checkMsgRate(getMsgCount());
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

		stopTiming();

		countSends(getMsgCount());
	}

	private synchronized void bumpMsgCount()
	{
		++currMsgCount;
	}
	private synchronized int getMsgCount()
	{
		return currMsgCount;
	}
	
	/**
	 * Create the message.
	 */
	private Message createMessage(Session session) throws JMSException {
		String payload = null;
		int bufferSize = 0;
		// create the message
		BytesMessage msg = session.createBytesMessage();

		// add the payload
		if (payloadFile != null) {
			try {
				InputStream instream = new BufferedInputStream(
						new FileInputStream(payloadFile));
				bufferSize = instream.available();
				byte[] bytesRead = new byte[bufferSize];
				instream.read(bytesRead);
				instream.close();

				payload = new String(bytesRead);

				if (payloadMinBytes > bufferSize) {
					logger.error("Payload file size (" + bufferSize
							+ ") < minimum msg size (" + payloadMaxBytes + ")");
					logger.error("Exiting.");
					System.exit(-1);
				}

				if (payloadMaxBytes > bufferSize) {
					logger.error("Payload file size (" + bufferSize
							+ ") < maximum msg size (" + payloadMaxBytes
							+ "). Setting maximum msg size to " + bufferSize);
					payloadMaxBytes = bufferSize;
				}

			} catch (IOException e) {
				logger.error("Error: unable to load payload file:", e);
			}

		}

		if (payloadMaxBytes > 0) {
			msgBuffer = new StringBuffer(payloadMaxBytes);
			char c = 'A';
			for (int i = 0; i < payloadMaxBytes; i++) {
				msgBuffer.append(c++);
				if (c > 'z')
					c = 'A';
			}
			payload = msgBuffer.toString();
		}

		if (payload != null) {
			payloadBytes = payload.getBytes();
			// add the payload to the message
			msg.writeBytes(payloadBytes);
		}

		return msg;
	}

	
/**
 * Compresses the Message body, if supported by the provider. Default
 * implementation is a no-op
 *  
 * @param msg - The message to be compressed
 * @return success/failure
 * @throws JMSException 
 */
	public void compressMessageBody(Message msg) throws JMSException
	{
		// No-op for generic JMSProducer
	}
	
	private synchronized void startTiming() {
		if (startTime == 0)
			startTime = System.currentTimeMillis();
	}

	private synchronized void stopTiming() {
		endTime = System.currentTimeMillis();
	}

	/**
	 * Get the performance results.
	 */
	private String getPerformance() {
		if (endTime > startTime) {
			elapsed = endTime - startTime;
			double seconds = elapsed / 1000.0;
			int perf = (int) ((sentCount * 1000.0) / elapsed);
			return (sentCount + " messages took " + seconds
					+ " seconds, performance is " + perf + " msg/sec " + "("
					+ perf / sessions + " msg/sec/session)");
		} else {
			return "interval too short to calculate a message rate";
		}
	}

	/**
	 * Get the total elapsed time.
	 */
	public long getElapsedTime() {
		return elapsed;
	}

	/**
	 * Get the total produced message msgGoal.
	 */
	public synchronized int getSentCount() {
		return sentCount;
	}

	/**
	 * Class used to control the producer's send rate.
	 */

	private class MsgRateChecker {
		private long sampleStart;
		private int sampleTime;
		private int sampleCount;
		private int _rate;

		MsgRateChecker(int rate) {
			_rate = rate;
			// initialize
			this.sampleTime = 10;
		}

		void checkMsgRate(int count) {
			if (_rate < 100) {
				if (count % 10 == 0) {
					try {
						long sleepTime = (long) ((10.0 / (double) _rate) * 1000);
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
					}
				}
			} else if (sampleStart == 0) {
				sampleStart = System.currentTimeMillis();
			} else {
				long elapsed = System.currentTimeMillis() - sampleStart;
				if (elapsed >= sampleTime) {
					int actualMsgs = count - sampleCount;
					int expectedMsgs = (int) (elapsed * ((double) _rate / 1000.0));
					if (actualMsgs > expectedMsgs) {
						long sleepTime = (long) ((double) (actualMsgs - expectedMsgs) / ((double) _rate / 1000.0));
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

	public void printConsoleBanner()
	{
		// print parameters
		System.err.println();
		System.err
				.println("------------------------------------------------------------------------");
		String className = JMSProducer.class.getName();
		className = className.substring(className.lastIndexOf('.')+1);
		System.err.println(className);
		System.err
				.println("------------------------------------------------------------------------");
		if (flavor != null)
			System.err.println("Broker Flavor................ " + flavor);
		System.err.println("JNDI Provider................ " + getBrokerURL());
		System.err.println("Initial Context Factory...... " + contextFactoryName);
		System.err.println("Connection Factory........... "
				+ connectionFactoryName);
		if (getUsername()!=null)
			System.err.println("User......................... " + getUsername());
		System.err.println("Destination.................. " + "(" + destType
				+ ") " + destName);
		System.err.println("Unique Destinations.......... " + uniqueDests);
		System.err.println("Timestamp Messages........... " + this.isTimestampEnabled());
		if (useRandomSize) {
			System.err.println("Min Message Size............. " + payloadMinBytes);
			System.err.println("Max Message Size............. "
					+ (payloadFile != null ? payloadFile : String
							.valueOf(payloadMaxBytes)));
		} else {
			System.err.println("Message Size................. "
					+ (payloadFile != null ? payloadFile : String
							.valueOf(payloadMaxBytes)));
		}
		if (msgGoal > 0)
			System.err.println("Count........................ " + msgGoal);
		else
			System.err.println("Count........................ " + msgGoal
					+ " (unlimited)");
	
		if (duration > 0)
			System.err.println("Duration..................... " + duration);
		else
			System.err.println("Duration..................... " + duration
					+ " (unlimited)");
	
		System.err.println("Producer Sessions............ " + sessions);
		System.err.println("Producer Connections......... " + connections);
		System.err.println("DeliveryMode................. "
				+ deliveryModes.inverseBidiMap().get(deliveryMode));
		System.err.println("Compression.................. " + compression);
		System.err.println("XA........................... " + xa);
		if (msgRate != null && msgRate > 0)
			System.err.println("Message Rate................. " + msgRate);
		else
			System.err.println("Message Rate................. " + msgRate
					+ " (unlimited)");
	
		if (getTxnSize() > 0)
			System.err.println("Transaction Size............. " + getTxnSize());
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println();		
	}

	/**
	 * @return the timestampEnabled
	 */
	protected synchronized boolean isTimestampEnabled() {
		return timestampEnabled;
	}

	/**
	 * @param timestampEnabled the timestampEnabled to set
	 */
	protected synchronized void setTimestampEnabled(boolean timestampEnabled) {
		this.timestampEnabled = timestampEnabled;
	}
}
