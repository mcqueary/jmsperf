package com.tibco.mcqueary.jmsperf;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class WorkerThread extends Thread {
	
	public static final String DEFAULT_NAME="WorkerThread";
	private static volatile boolean debugLifecycle=false;
	private static final AtomicInteger created = new AtomicInteger();
	private static final AtomicInteger alive = new AtomicInteger();
	private static Logger logger;

	public WorkerThread(Runnable r) {
		this(r, DEFAULT_NAME);
	}

	public WorkerThread(Runnable runnable, String name) {
		super(runnable, name + "-" + created.incrementAndGet());
		logger = Logger.getLogger(this.getClass());
		setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				// TODO Auto-generated method stub
				logger.error("UNCAUGHT in thread " + t.getName(), e);

			}
		});
	}
	
	public void run()
	{
		boolean debug = debugLifecycle;
		if (debug)
			logger.debug("Created " + getName());
		try {
			alive.incrementAndGet();
			super.run();
		} finally {
			alive.decrementAndGet();
			if (debug) logger.debug("Exiting " + getName());
		}
	}
	
	public static int getThreadsCreated() { return created.get(); }
	public static int getThreadsAlive() { return alive.get(); }
	public static boolean getDebug() { return debugLifecycle; }
	public static void setDebug(boolean b) { debugLifecycle=b; }
}
