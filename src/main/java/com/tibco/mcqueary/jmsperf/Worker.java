package com.tibco.mcqueary.jmsperf;

import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author Larry McQueary
 *
 */
public interface Worker {
	
	/**
	 * Setup or acquire all need resources (threads, connections, etc)
	 * required by the configuration 
	 */
	public void setup();
	
	/**
	 * Signal the worker to begin execution
	 */
	public void startup(); 

	/**
	 * Signal the worker to pause indefinitely.
	 */
	public void pause();

	/**
	 * Signal the worker to pause for specific period
	 * 
	 * @param msec Number of milliseconds to pause 
	 */

	public void pause(int msec);

	/**
	 *  Tells the worker to resume execution
	 */
	public void resume();

	/**
	 * Signal the worker to stop execution and reset state, without 
	 * releasing system resources.
	 * 
	 */
	public void reset();

	/**
	 * Shut down gracefully, releasing any resources.
	 */
	public void end();

	/**
	 * @return TRUE if the worker is running. FALSE if the worker is stopped.
	 */
	public boolean isRunning();

	/**
	 * @return TRUE if the worker is currently paused. FALSE if it is not.
	 */
	public boolean isPaused();
	
	public PropertiesConfiguration getConfig();
}
