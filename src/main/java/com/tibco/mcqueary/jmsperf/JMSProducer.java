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
 *   -count        <num msgs>    Number of messages to send. Default is 10k.
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

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class JMSProducer extends JMSWorker implements Runnable {

	public static final String PROPERTY_FILE = "producer.properties";
	
	public static BidiMap<String,Integer> deliveryModes = 
            new DualHashBidiMap<String, Integer>();
	
	// parameters
	private String payloadFile = null;
	private boolean compression = false;
	private Integer msgRate = null;
	private Integer minMsgSize = null;
	private Integer maxMsgSize = null;
	private Integer deliveryMode = null;
	private StringBuffer msgBuffer = null;
	private byte[] payloadBytes = null;

	// variables
	private int sentCount;
	private long startTime;
	private long endTime;
	private long elapsed;
	private boolean stopNow;

	private boolean useRandomSize = false;
	private Random rand = null;

	// create the producer threads
	/**
	 * Constructor
	 * 
	 * @param args
	 *            the command line arguments
	 */
	public JMSProducer(Configuration input) throws NoSuchElementException {
		super(input);
//		PropertiesConfiguration defaultConfig = new PropertiesConfiguration();

		Manager.listConfig("JMSProducer constructor", this.config);
		
		deliveryModes.put("NON_PERSISTENT", DeliveryMode.NON_PERSISTENT);
		deliveryModes.put("PERSISTENT", DeliveryMode.PERSISTENT);
	
		
		payloadFile = config.getString(Manager.PROP_PRODUCER_PAYLOAD_FILENAME);
		compression = config.getBoolean(Manager.PROP_PRODUCER_COMPRESSION);
		msgRate = config.getInt(Manager.PROP_PRODUCER_RATE,0);
		txnSize = config.getInt(Manager.PROP_TRANSACTION_SIZE);
		count = config.getInt(Manager.PROP_MESSAGE_COUNT);
		runTime = config.getInt(Manager.PROP_DURATION);
		minMsgSize = config.getInt(Manager.PROP_PRODUCER_PAYLOAD_MINSIZE);
		maxMsgSize = config.getInt(Manager.PROP_PRODUCER_PAYLOAD_MAXSIZE);
		if (minMsgSize < maxMsgSize)
			useRandomSize = true;
		deliveryMode = deliveryModes.get(config.getString(Manager.PROP_PRODUCER_DELIVERY_MODE));
		
		// print parameters
		System.err.println();
		System.err
				.println("------------------------------------------------------------------------");
		String className = JMSProducer.class.getName();
		className = className.substring(className.lastIndexOf('.')+1);
		System.err.println(className);
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println("Broker Flavor................ " + flavor);
		System.err.println("JNDI Provider................ " + jndiProviderURL);
		System.err.println("Initial Context Factory...... " + contextFactoryName);
		System.err.println("Connection Factory........... "
				+ connectionFactoryName);
		System.err.println("User......................... " + username);
		System.err.println("Destination.................. " + "(" + destType
				+ ") " + destName);
		System.err.println("Unique Destinations.......... " + uniqueDests);
		if (useRandomSize) {
			System.err.println("Min Message Size............. " + minMsgSize);
			System.err.println("Max Message Size............. "
					+ (payloadFile != null ? payloadFile : String
							.valueOf(maxMsgSize)));
		} else {
			System.err.println("Message Size................. "
					+ (payloadFile != null ? payloadFile : String
							.valueOf(maxMsgSize)));
		}
		if (count != null && count > 0)
			System.err.println("Count........................ " + count);
		else
			System.err.println("Count........................ " + count
					+ " (unlimited)");

		if (runTime != null && runTime > 0)
			System.err.println("Duration..................... " + runTime);
		else
			System.err.println("Duration..................... " + runTime
					+ " (unlimited)");

		System.err.println("Producer Sessions............ " + sessions);
		System.err.println("Producer Connections......... " + connections);
		System.err.println("DeliveryMode................. "
				+ deliveryModes.get(deliveryMode));
		System.err.println("Compression.................. " + compression);
		System.err.println("XA........................... " + xa);
		if (msgRate != null && msgRate > 0)
			System.err.println("Message Rate................. " + msgRate);
		else
			System.err.println("Message Rate................. " + msgRate
					+ " (unlimited)");

		if (txnSize!=null && txnSize > 0)
			System.err.println("Transaction Size............. " + txnSize);
		System.err
				.println("------------------------------------------------------------------------");
		System.err.println();
	}

	public void start() {
		try {

			if (!xa)
				createConnectionFactoryAndConnections();
			else
				createXAConnectionFactoryAndXAConnections();

			// create the producer threads
			Vector<Thread> tv = new Vector<Thread>(sessions);
			for (int i = 0; i < sessions; i++) {
				Thread t = new Thread(this);
				tv.add(t);
				t.start();
				System.err.print(String.format("\r    sessions created: %6d",
						tv.size()));
			}
			System.err.println("\nPRODUCING");

			// run for the specified amount of time
			if (runTime > 0) {
				try {
					Thread.sleep(runTime * 1000);
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
			cleanup();

			// print performance
			LOGGER.info(getPerformance());
		} catch (NamingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Update the total sent count.
	 */
	private synchronized void countSends(int count) {
		sentCount += count;
	}

	/**
	 * The producer thread's run method.
	 */
	public void run() {
		int msgCount = 0;
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
			msgProducer.setDisableMessageTimestamp(true);

			// create the message
			Message msg = createMessage(session);

			// enable compression if necessary
			if (compression)
				msg.setBooleanProperty("JMS_TIBCO_COMPRESS", true);

			// initialize message rate checking
			if (msgRate > 0)
				msgRateChecker = new MsgRateChecker(msgRate);

			startTiming();

			int randomSize = 0;

			// publish messages
			while ((count == 0 || msgCount < (count / sessions)) && !stopNow) {
				// a no-op for local txns
				txnHelper.beginTx(xaResource);

				if (useRandomSize) {
					if (rand == null)
						rand = new Random();
					randomSize = rand.nextInt(maxMsgSize - minMsgSize + 1)
							+ minMsgSize;
					// LOGGER.trace("Generating message body of size " +
					// randomSize + " bytes.");
					msg.clearBody();
					try {
						((BytesMessage) msg).writeBytes(payloadBytes, 0,
								randomSize);
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error(
								"Caught IndexOutOfBoundsException while writng message body:"
										+ "	payloadBytes.length == "
										+ payloadBytes.length
										+ "	randomSize == " + randomSize, e);
					}
				}
				msgProducer.send(msg);

				msgCount++;

				// commit the transaction if necessary
				if (txnSize > 0 && msgCount % txnSize == 0)
					txnHelper.commitTx(xaResource, session);

				// check the message rate
				if (msgRate > 0)
					msgRateChecker.checkMsgRate(msgCount);
			}

			// commit any remaining messages
			if (txnSize > 0)
				txnHelper.commitTx(xaResource, session);
		} catch (JMSException e) {
			if (!stopNow) {
				LOGGER.error("JMSException: ", e);
				Exception le = e.getLinkedException();
				if (le != null) {
					LOGGER.error("Linked exception: ", le);
				}
			}
		}

		stopTiming();

		countSends(msgCount);
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

				if (minMsgSize > bufferSize) {
					LOGGER.error("Payload file size (" + bufferSize
							+ ") < minimum msg size (" + maxMsgSize + ")");
					LOGGER.error("Exiting.");
					System.exit(-1);
				}

				if (maxMsgSize > bufferSize) {
					LOGGER.error("Payload file size (" + bufferSize
							+ ") < maximum msg size (" + maxMsgSize
							+ "). Setting maximum msg size to " + bufferSize);
					maxMsgSize = bufferSize;
				}

			} catch (IOException e) {
				LOGGER.error("Error: unable to load payload file:", e);
			}

		}

		if (maxMsgSize > 0) {
			msgBuffer = new StringBuffer(maxMsgSize);
			char c = 'A';
			for (int i = 0; i < maxMsgSize; i++) {
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
					+ perf / sessions + " msg/sec per thread)");
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
	 * Get the total produced message count.
	 */
	public int getSentCount() {
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
