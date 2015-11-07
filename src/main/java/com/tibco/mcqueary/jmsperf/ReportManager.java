package com.tibco.mcqueary.jmsperf;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.log4j.Logger;

import static com.tibco.mcqueary.jmsperf.Constants.*;

public class ReportManager implements Runnable {
	private MemoryUsage heapUsage = null;
	private MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
	
	private long interval = 0;
	private long startTime = 0;
	private long stopTime = 0;
	private long lastSnapshotMsgCount = 0;
	private long msgLimit = 0;
	private boolean started = false;
	private AtomicLong currMsgCount = new AtomicLong(0);
	private AtomicLong latencySamples = new AtomicLong(0);
	private AtomicLong latencyTotal = new AtomicLong(0);
	private AtomicLong rtLatencySamples = new AtomicLong(0);
	private AtomicLong rtLatencyTotal = new AtomicLong(0);
	private AtomicBoolean done = new AtomicBoolean(false);
	private int offset = 0;
	private int warmup = 0;
	private int sessions;
	long totalElapsedSeconds = 0L;

	private static Logger logger = Logger.getLogger(ReportManager.class);
	
	ReportManager(int howOften, long howMany, int numSessions, 
			int warmup, int offset) {
		this.interval = howOften;
		this.msgLimit = howMany;
		this.sessions = numSessions;
//		this.currMsgCount = msgCounter;
		this.warmup = warmup;
		this.offset = offset;
	}

	protected synchronized long getMsgCount()
	{
		return currMsgCount.get();
	}
	
	protected synchronized long incrementMsgCount()
	{
		return currMsgCount.incrementAndGet();
	}
	
	protected synchronized void startTiming() {
		if (!started) {
			started = true;
			this.startTime = System.currentTimeMillis();
			logger.debug("Timing started");
		}
	}

	protected synchronized void stopTiming() {
		if (started) {
			started = false;
			this.stopTime = System.currentTimeMillis();
			finalStats();
			reportOverallPerformance();
			this.done.set(true);
		}
	}

	protected synchronized void finalStats() {
		// report final stats
		long elapsedSecs = (this.stopTime - this.startTime) / 1000;
		if (currMsgCount.get() > 0) {
			this.reportIntervalStatus(elapsedSecs);
		}
	}

	protected synchronized long getMaxMemory() {
		this.heapUsage = memoryBean.getHeapMemoryUsage();
		return (heapUsage.getMax() / MEGABYTE);
	}

	protected synchronized long getUsedMemory() {
		this.heapUsage = memoryBean.getHeapMemoryUsage();
		return (heapUsage.getUsed() / MEGABYTE);
	}

	protected synchronized void reportIntervalStatus(long elapsed) {
		long msgsThisInterval = currMsgCount.get() - lastSnapshotMsgCount;
//		long totalElapsed = System.currentTimeMillis() - this.startTime;
		double averageLatency = 0.0;
		double averageRTLatency = 0.0;
		if (latencySamples.get() > 0)
			averageLatency = ( (double) latencyTotal.get() /  (double) latencySamples.get());
		
		if (rtLatencySamples.get() > 0)
		{
			averageRTLatency = (rtLatencyTotal.get() /rtLatencySamples.get())/1000.0;
		}

		if ((msgsThisInterval > 0) && (elapsed > 0)) {
			// long currMsgRate = (long) (msgsThisInterval / elapsed);
			long msgs = currMsgCount.get();
			long max = msgLimit;
			long cumulative = msgs / totalElapsedSeconds;
			String msgTotalString = null;
			String format = "%" + String.valueOf(this.msgLimit).length() + "d";
			if (this.msgLimit > 0)
				msgTotalString = String.format(format + "/" + format
						+ " msgs, ", msgs, max);
			else
				msgTotalString = String.format(format + "/unlimited msgs, ",
						msgs);

			String reportString = msgTotalString;
			// reportString += "(" + currMsgRate + " msg/sec last "+ elapsed
			// +" sec), ";
			reportString += "thru=" + cumulative + " msg/s, ";
			if (latencySamples.get() > 0) {
				reportString += "rcvlat=" + String.format("%.3f", averageLatency)
						+ "ms, ";
				if (offset != 0)
					reportString += "(offset=" + offset + "ms), ";
			}
			if (rtLatencySamples.get() > 0)
			{
				reportString += "rtt=" + String.format("%.3f", averageRTLatency)
						+ "ms, ";
			}

			reportString += "mem=" + getUsedMemory() + "MB/" + getMaxMemory()
					+ "MB";
			logger.info(reportString);

			lastSnapshotMsgCount = currMsgCount.get();
		} else {
			logger.debug("Reporting interval elapsed but nothing to report");
		}
	}

	protected synchronized void recordLatency(long t0, Message msg) throws JMSException
	{
		long startNanos = t0;
		long endNanos = 0L;
		if (startNanos > 0)
		{
			endNanos = System.nanoTime();
			// we store the roundtrip latency in microseconds accuracy
			this.rtLatencyTotal.addAndGet((endNanos-startNanos)/1000);
			this.rtLatencySamples.incrementAndGet();
		}
		
		long now = System.currentTimeMillis();
		long msgTimestamp = msg.getJMSTimestamp() + this.offset;
		if ((totalElapsedSeconds > warmup) && (msgTimestamp !=0 ))
		{
			this.latencySamples.incrementAndGet();
			this.latencyTotal.addAndGet(now-msgTimestamp);
		}
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	/**
	 * Get the performance results.
	 */
	protected synchronized void reportOverallPerformance() {
		long elapsed = this.stopTime - this.startTime;
		double averageLatency = 0.0;
		if (latencySamples.get() > 0)
			averageLatency = ((double)latencyTotal.get() / (double)latencySamples.get());

		if (elapsed > 0) {
			double seconds = elapsed / 1000.0;
			double avgThroughput = round(((currMsgCount.get() * 1000.0) / elapsed), 0);

			String message = currMsgCount + " messages took " + seconds
					+ " seconds";
			message += ", avg throughput=" + avgThroughput + "msg/sec";
			message += "(" + avgThroughput / sessions + " msg/sec/session)";
			if (latencySamples.get() > 0)
				message += ", avg latency="
						+ String.format("%fms", averageLatency);
			logger.info(message);
		} else {
			logger.info("interval too short to calculate a message rate");
		}
	}

	public void run() {
		logger.trace("Reporting manager started.");
		while (currMsgCount.get() == 0) {

		}
		int warmupSeconds = 0;
		if (warmup > 0)
			logger.info("Warming up for " + warmup + " seconds");
		while (warmupSeconds++ < warmup) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}

		long start = System.currentTimeMillis();
		while (!done.get()) {
			long current = System.currentTimeMillis();
			totalElapsedSeconds = (current - this.startTime)/1000; 
			long elapsed = (current - start) / 1000;
			if (elapsed == this.interval) {
				this.reportIntervalStatus(elapsed);
				start = current;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
//		this.stopTiming();
	}
}