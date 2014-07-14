package com.tibco.mcqueary.jmsperf;

public interface Builder<T extends JMSClient> {
	public T build();
}
