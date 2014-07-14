package com.tibco.mcqueary.jmsperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

public class JMSWorkerThreadFactory implements ThreadFactory {
	private final String poolName;
	private CountDownLatch startSignal;
	private CountDownLatch doneSignal;

	public JMSWorkerThreadFactory(String poolName)
	{
		this(poolName,null,null);
	}
	
	public JMSWorkerThreadFactory(String poolName, CountDownLatch startSignal,
			CountDownLatch doneSignal)
	{
		this.poolName = poolName;
		this.startSignal=startSignal;
		this.doneSignal=doneSignal;
	}
	
	public Thread newThread(Runnable runnable, CountDownLatch startSignal,
			CountDownLatch doneSignal) {
		return new JMSWorkerThread(runnable, poolName, startSignal, doneSignal);
	}

	@Override
	public Thread newThread(Runnable r) {
		return newThread(r, null, null);
	}

}