package com.tibco.mcqueary.jmsperf;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class JMSWorkerThread extends WorkerThread {
		public static final String DEFAULT_NAME="JMSWorkerThread";
		private CountDownLatch startSignal=null;
		private CountDownLatch doneSignal=null;
		private Logger logger=Logger.getLogger(JMSWorkerThread.class);
		
		public JMSWorkerThread(Runnable r) {
			this(r, DEFAULT_NAME);
		}

		public JMSWorkerThread(Runnable r, String poolName)
		{
			this(r, poolName, null, null);
		}

		public JMSWorkerThread(Runnable r, String poolName, CountDownLatch 
				startSignal, CountDownLatch doneSignal)
		{
			super(r, poolName);
			this.startSignal=startSignal;
			this.doneSignal=doneSignal;
		}
		
		public void run()
		{
			try {
				if (startSignal != null)
					startSignal.await();
				super.run();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.trace("Interrupted: ", e);
			} finally {
				if (this.doneSignal != null)
					this.doneSignal.countDown();
			}
			
		}
	}