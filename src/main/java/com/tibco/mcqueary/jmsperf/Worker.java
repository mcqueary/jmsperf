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
	 * Signal the client to begin execution
	 */
	public void startup(); 

	/**
	 * Signal the client to pause indefinitely.
	 */
	public void pause();

	/**
	 * Signal the client to pause for specific period
	 * 
	 * @param msec Number of milliseconds to pause 
	 */

	public void pause(int msec);

	/**
	 *  Tells the client to resume execution
	 */
	public void resume();

	/**
	 * Signal the client to stop execution and reset state, without 
	 * releasing system resources.
	 * 
	 */
	public void reset();

	/**
	 * Shut down gracefully, releasing any resources.
	 */
	public void end();

	/**
	 * @return TRUE if the client is running. FALSE if the client is stopped.
	 */
	public boolean isRunning();

	/**
	 * @return TRUE if the client is currently paused. FALSE if it is not.
	 */
	public boolean isPaused();
	
	public PropertiesConfiguration getConfig();
}
