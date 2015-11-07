package com.tibco.mcqueary.jmsperf;

import javax.naming.NamingException;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.AbstractProducer.Builder;

public class Producer extends AbstractProducer {

	public Producer(PropertiesConfiguration inputConfig)
			throws IllegalArgumentException, NamingException {
		super(inputConfig);
		
		printConsoleBanner();
	}

	public Producer(Builder builder) {
		super(builder);
		// TODO Auto-generated constructor stub
	}

	public static class Builder extends AbstractProducer.Builder<Builder>
	{
		public Builder me() { return this; }
		
		public Producer build()
		{
			return new Producer(this);
		}
	}
	
}
