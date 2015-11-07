package com.tibco.mcqueary.jmsperf;

import javax.naming.NamingException;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.tibco.mcqueary.jmsperf.AbstractConsumer.Builder;

public class Consumer extends AbstractConsumer {
	protected Consumer(PropertiesConfiguration config) throws IllegalArgumentException, NamingException{
		super(config);
		
		printConsoleBanner();
	}
	
	protected Consumer(Builder builder)
	{
		super(builder);
		
		printConsoleBanner();
	}
	
	public static class Builder extends AbstractConsumer.Builder<Builder>
	{
		public Builder () {}
		
		@Override
		protected Builder me() {
			return this;
		}
		
		public Consumer build() {
			return new Consumer(this);
		}
	}
}
