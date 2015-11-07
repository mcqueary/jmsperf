/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.naming.NamingException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.JMSClient.Flavor;

/**
 * @author Larry McQueary
 *
 */
public class TIBEMSConsumer extends JMSConsumer {

	final static int TIBCO_NO_ACKNOWLEDGE= 22;
	final static int TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE= 23;
	final static int TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE= 24;
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
		flavor = Flavor.TIBEMS;
		ackModes.put("TIBCO_NO_ACKNOWLEDGE", TIBCO_NO_ACKNOWLEDGE );
		ackModes.put("TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE", TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE);
		ackModes.put("TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE", TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE);
		brokerURL = DEFAULT_PROVIDER_URL;
		contextFactoryName = DEFAULT_CONTEXT_FACTORY;
		connectionFactoryName = DEFAULT_CONNECTION_FACTORY;

	}
	
	@Override
	public void acknowledge(Message msg, int mode) throws JMSException
	{
		if ((mode == Session.CLIENT_ACKNOWLEDGE) ||(mode == TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE)
				|| (mode == TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE)) {
			msg.acknowledge();
		}
	}
	
	@Override
	public void postProcess(Message msg) throws JMSException
	{
		// force the uncompression of compressed messages
		if (msg.getBooleanProperty("JMS_TIBCO_COMPRESS"))
			((BytesMessage) msg).getBodyLength();
	}

}
