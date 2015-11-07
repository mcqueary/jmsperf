/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import java.util.NoSuchElementException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.NamingException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author Larry McQueary
 *
 */
public class TIBEMSProducer extends JMSProducer {

	static final Flavor flavor = Flavor.TIBEMS;

	final static int TIBCO_RELIABLE = 22;

	/**
	 * @param input
	 * @throws ConfigurationException 
	 * @throws NoSuchElementException 
	 * @throws NamingException 
	 * @throws IllegalArgumentException 
	 */
	public TIBEMSProducer(PropertiesConfiguration input) throws NoSuchElementException, ConfigurationException, IllegalArgumentException, NamingException {
		super(input);
		// TODO Auto-generated constructor stub
	    deliveryModes.put("TIBCO_RELIABLE", TIBCO_RELIABLE);

	}
	
	@Override
	public void compressMessageBody(Message msg) throws JMSException
	{
		msg.setBooleanProperty("JMS_TIBCO_COMPRESS", true);
	}

}
