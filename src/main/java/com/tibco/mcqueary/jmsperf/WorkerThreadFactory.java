package com.tibco.mcqueary.jmsperf;

import java.util.concurrent.ThreadFactory;

public class WorkerThreadFactory implements ThreadFactory {
	private final String poolName;

	public WorkerThreadFactory(String poolName)
	{
		this.poolName = poolName;
	}
	
	@Override
	public Thread newThread(Runnable runnable) {
		// TODO Auto-generated method stub
		return new WorkerThread(runnable, poolName);
	}

}
