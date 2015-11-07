package com.tibco.mcqueary.jmsperf;

import javax.jms.*;
import javax.naming.NamingException;

import org.apache.commons.configuration.*;

public class PooledConsumer extends AbstractConsumer {
	private Connection connection = null;
	private ConnectionConsumer connectionConsumer = null;
	private Session session = null;
	private Destination destination = null;
	private ServerSessionPool sessionPool = null;
	
	private final static int MAX_MESSAGES = 1000;
	
	public PooledConsumer(PropertiesConfiguration config) 
			throws IllegalArgumentException, NamingException {
		super(config);
		
		try {
			this.connection = this.connectionFactory.createConnection(username, password);
			this.session = connection.createSession((this.txnSize > 0), this.ackMode.value());
			switch (destType)
			{
			case TOPIC:
				this.destination = session.createTopic(destName);
				break;
			case QUEUE:
				this.destination = session.createQueue(destName);
				break;
			default:
					break;
			}
			this.connectionConsumer = 
					connection.createConnectionConsumer(this.destination, 
							this.selector, sessionPool, MAX_MESSAGES); 
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override 
	public void setup() {
		
	}
	
	public ServerSession createServerSession()
	{
		ServerSession srvSess = null;

		//		ServerSession srvSess = new ServerSession();
		
		return srvSess;
		
	}
}
