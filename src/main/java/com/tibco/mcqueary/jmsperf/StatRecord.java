/**
 * 
 */
package com.tibco.mcqueary.jmsperf;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * @author Larry McQueary
 *
 */
public class StatRecord {
	
	public long timeSent;
	public long timeReceived;
	public long threadID;
	public String clientID=null;
	public int sessionNum;
	public String messageID=null;
	public long latency;
	
	public StatRecord(Message msg) throws JMSException
	{
		this(msg, null, 0);
	}
	
	public StatRecord(Message msg, Connection conn, int sessNum) throws JMSException
	{
		this.timeReceived=System.currentTimeMillis();
		this.timeSent=msg.getJMSTimestamp();
		this.clientID=conn.getClientID();
		this.sessionNum=sessNum;
		this.latency=this.timeReceived-this.timeSent;
	}
}
