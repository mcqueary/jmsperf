/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.naming.NamingException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.Constants.AckMode;

import static com.tibco.mcqueary.jmsperf.Constants.*;

/**
 * @author Larry McQueary
 *
 */
public class TIBEMSConsumer extends Consumer {

	final static String DEFAULT_CONTEXT_FACTORY = "com.tibco.tibjms.naming.TibjmsInitialContextFactory";
	final static String DEFAULT_PROVIDER_URL = "tibjmsnaming://localhost:7222";
	final static String DEFAULT_CONNECTION_FACTORY = "ConnectionFactory";

	/**
	 * @param inputConfig
	 * @throws ConfigurationException 
	 * @throws NamingException 
	 * @throws IllegalArgumentException
	 */
	public TIBEMSConsumer(PropertiesConfiguration inputConfig) throws ConfigurationException, IllegalArgumentException, NamingException {
		super(inputConfig);
		provider = Provider.TIBEMS;
		brokerURL = DEFAULT_PROVIDER_URL;
		contextFactoryName = DEFAULT_CONTEXT_FACTORY;
		connectionFactoryName = DEFAULT_CONNECTION_FACTORY;

		ackModes.add(AckMode.TIBCO_NO_ACKNOWLEDGE);
		ackModes.add(AckMode.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE);
		ackModes.add(AckMode.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE);
		
		for (AckMode a : ackModes)
		{
			System.err.println(a);
		}
	}
	
	public TIBEMSConsumer(Builder builder)
	{
		super(builder);
		provider = Provider.TIBEMS;		
		printConsoleBanner();
	}
	
	@Override
	public void acknowledge(Message msg, int mode) throws JMSException
	{
		if ((mode == Session.CLIENT_ACKNOWLEDGE) ||(mode == AckMode.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE.value())
				|| (mode == AckMode.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE.value())) {
			msg.acknowledge();
		}
	}
	
	@Override
	public void process(Message msg) throws JMSException
	{
		// force the uncompression of compressed messages
		if (msg.getBooleanProperty("JMS_TIBCO_COMPRESS"))
			((BytesMessage) msg).getBodyLength();
	}

}
