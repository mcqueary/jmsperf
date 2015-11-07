package com.tibco.mcqueary.jmsperf;

import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.NamingException;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.Consumer.Builder;

import static com.tibco.mcqueary.jmsperf.Constants.*;

public class Responder extends AbstractConsumer {

	private Session replySession = null;
	private Connection replyConnection = null;
	private MessageProducer replyProducer = null;
	protected boolean timestampEnabled =  PROP_PRODUCER_TIMESTAMP_DEFAULT;

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

	public Responder(PropertiesConfiguration config)
			throws IllegalArgumentException, NamingException, JMSException {
		super(config);
		
		replyConnection = connectionFactory.createConnection(this.username, this.password);
		replySession = replyConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		replyProducer = replySession.createProducer(null);
		
		timestampEnabled = config.getBoolean(PROP_PRODUCER_TIMESTAMP, timestampEnabled);
		
		replyProducer.setDisableMessageTimestamp(!this.isTimestampEnabled());
		
		printConsoleBanner();
	}
	
	public Responder(Builder builder) {
		super(builder);
	}

	@Override
	public void setup()
	{
		super.setup();
		
	}
	
	@Override
	protected void process(Message receivedMessage) 
			throws JMSException, IllegalArgumentException, IOException
	{
		super.process(receivedMessage);
		
		BytesMessage msg = (BytesMessage) receivedMessage;
		Destination replyToDest = msg.getJMSReplyTo();
		if (replyToDest == null)
			throw new IllegalArgumentException("Received message has no reply-to field");
		else {
			BytesMessage replyMsg = replySession.createBytesMessage();
			int length = (int)msg.getBodyLength();
			if (length > 0)
			{
				byte[] payload = new byte[length];
				int bytesRead = msg.readBytes(payload, length);
				if (bytesRead != length)
					throw new IOException("Couldn't read all bytes from incoming message body");
				replyMsg.writeBytes(payload);
			}
		
			String correlationID = msg.getJMSCorrelationID();
			if (correlationID != null)
			{
				replyMsg.setJMSCorrelationID(correlationID);
			}
			else if (msg.getJMSMessageID() != null)
			{
				replyMsg.setJMSCorrelationID(msg.getJMSMessageID());
			}
//			replyMsg.setJMSDestination(replyToDest);
			replyProducer.send(replyToDest, replyMsg);
		}
	}
	
	@Override
	public void printSpecificSettings()
	{
		super.printSpecificSettings();
		System.err.println("Timestamp Messages........... "
				+ this.isTimestampEnabled());
	}
	
	public static class Builder extends AbstractConsumer.Builder<Builder>
	{
		public Builder () {}
		
		@Override
		protected Builder me() {
			return this;
		}
		
		public Responder build() {
			return new Responder(this);
		}
	}
}
