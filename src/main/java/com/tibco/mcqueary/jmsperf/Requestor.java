package com.tibco.mcqueary.jmsperf;

import java.util.UUID;

import javax.jms.*;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.Client.JMSPerfTxnHelper;
import com.tibco.mcqueary.jmsperf.Producer.Builder;
import com.tibco.mcqueary.jmsperf.AbstractProducer.MsgRateChecker;

public class Requestor extends AbstractProducer {

//	private Destination replyToDest = null;
//	private Session replySession = null;
//	private Connection replyConnection = null;
	
	public Requestor(PropertiesConfiguration inputConfig)
			throws IllegalArgumentException, NamingException {
		super(inputConfig);
		printConsoleBanner();
	}

	public Requestor(Builder builder) {
		super(builder);
		printConsoleBanner();
	}

	@Override
	public void setup() {
		super.setup();
	}
	
	@Override 
	protected synchronized void preProcess(BytesMessage msg)
	{
		
	}
	
	@Override
	protected synchronized void postProcess(BytesMessage msg)
	{
		
	}
	
	@Override
	public void run() {
		boolean started = false;
		MsgRateChecker msgRateChecker = null;

		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}

		try {
			MessageProducer msgProducer = null;
			MessageConsumer replyConsumer = null;
			Destination destination = null;
			Session session = null;
			XAResource xaResource = null;
			JMSPerfTxnHelper txnHelper = getPerfTxnHelper(xa);
			Destination replyToDest = null;
			
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

			replyToDest = session.createTemporaryQueue();

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

			int randomSize = 0;

			// publish messages
			while ((msgGoal == 0 || reportManager.getMsgCount() < (msgGoal / sessions))
					&& !stopNow) {
				// a no-op for local txns
				txnHelper.beginTx(xaResource);

				payloadBytes = this.createPayload(payloadFileName, randomSize);
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
								"Caught IndexOutOfBoundsException while writing message body:"
										+ "	payloadBytes.length == "
										+ payloadBytes.length
										+ "	randomSize == " + randomSize, e);
					}
				}

				if (reportManager.getMsgCount() < msgGoal) {
					msg.setJMSReplyTo(replyToDest);
					String correlationID=null;
					if (!isMessageIDEnabled())
					{
						correlationID = UUID.randomUUID().toString();
						msg.setJMSCorrelationID(correlationID);
					}
					
					if (!started)
					{
						reportManager.startTiming();
						started=true;
						logger.trace("Started processing");
	 				}

					long t0 = System.nanoTime();
					msgProducer.send(msg);
					if (isMessageIDEnabled())
						correlationID = msg.getJMSMessageID();

					replyConsumer = session.createConsumer(replyToDest, "JMSCorrelationID='"+correlationID+"'");

					BytesMessage replyMsg = (BytesMessage) replyConsumer.receive();
					reportManager.incrementMsgCount();

					reportManager.recordLatency(t0, replyMsg);
					
					replyConsumer.close();
				}

				// commit the transaction if necessary
				if (txnSize > 0 && (reportManager.getMsgCount() % txnSize == 0))
					txnHelper.commitTx(xaResource, session);

				// check the message rate
				if (msgRate > 0)
					msgRateChecker.throttleMsgRate(reportManager.getMsgCount());
			}

			reportManager.stopTiming();

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

	}
	
	public static class Builder extends AbstractProducer.Builder<Builder>
	{
		public Builder me() { return this; }
		
		public Requestor build()
		{
			return new Requestor(this);
		}
	}
	
}
