package com.tibco.mcqueary.jmsperf;

/* 
 * Copyright (c) 2001-$Date: 2009-09-11 14:08:04 -0700 (Fri, 11 Sep 2009) $ TIBCO Software Inc. 
 * All rights reserved.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: jmsPerfCommon.java 41997 2009-09-11 21:08:04Z bmahurka $
 * 
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

public class jmsPerfCommon
{
    protected int    connections = 1;
    protected Vector connsVector;
    
    protected String jndiProviderURL = null;
    protected String username = null;
    protected String password = null;
    protected String durableName = null;
    protected String destType = "topic";
    protected String destName = "topic.sample";
    protected String factoryName = null;
    protected boolean debug=false;

//    protected boolean useTopic = true;
    protected boolean uniqueDests = false;

    protected int connIter = 0;
    protected int destIter = 0;
    protected int nameIter = 0;
    protected boolean xa = false;

    protected static Properties props = null;
    
    protected static String DESTTYPE_TOPIC = "topic";
    protected static String DESTTYPE_QUEUE = "queue";
    
    protected static String PROVIDER_FLAVOR = "flavor";
    public static String PROVIDER_JNDI = "java.naming.provider.url";
    public static String PROVIDER_ICF = "java.naming.factory.initial";
    protected static String USERNAME = "java.naming.security.principal";
    protected static String PASSWORD = "java.naming.security.credentials";

    protected static String FACTORY = "factory";
    protected static String DESTINATION_TYPE = "destination.type";
    protected static String DESTINATION_NAME = "destination.name";
    protected static String COUNT = "count";
    protected static String DURATION = "duration";
    
    protected static String COMPRESSION = "compression";
    protected static String UNIQUE_DESTS = "uniquedests";
    protected static String USE_XA = "use_xa";
    protected static String TXNSIZE = "txnsize";

    protected static String DEBUG = "debug";

    // Consumer options
    protected static String CONSUMER_THREADS = "consumer.threads";
    protected static String CONSUMER_CONNECTIONS = "consumer.connections";
    protected static String CONSUMER_DURABLE_NAME = "consumer.durable.name";
    protected static String CONSUMER_ACK_MODE = "consumer.ackmode";
    protected static String CONSUMER_SELECTOR = "consumer.selector";
    // Producer options
    protected static String PRODUCER_THREADS = "producer.threads";
    protected static String PRODUCER_CONNECTIONS = "producer.connections";
    protected static String PRODUCER_PAYLOAD_FILE = "producer.payload.file";
    protected static String PRODUCER_PAYLOAD_MINSIZE = "producer.payload.minsize";
    protected static String PRODUCER_PAYLOAD_MAXSIZE = "producer.payload.maxsize";
    protected static String PRODUCER_DELIVERY_MODE = "producer.delivery_mode";
    protected static String PRODUCER_MESSAGE_RATE = "producer.rate";
    

    protected static enum Flavor { TIBEMS, WMQ, HORNETQ, SWIFTMQ, ACTIVEMQ, QPID, OPENMQ };
    protected static Flavor flavor;
    
    public jmsPerfCommon() {}

    protected void initProperties(String propfile)
    {
    	Properties p = new Properties();
    	try {
    	  p.load(new FileInputStream(propfile));
    	} catch (IOException e) {
    	  e.printStackTrace();
    	  return;
    	}
    	if (props==null)
    		props=p;
    	else
    	{
    		props.clear();
    		props=p;
    	}
        if (debug)
        {
	        Enumeration e = props.propertyNames();
	        while (e.hasMoreElements()) {
	          String key = (String) e.nextElement();
	          System.out.println(key + " -- " + props.getProperty(key));
	        }
        }
    }
    
    /**
     * Convert acknowledge mode to a string.
     */
    protected static String ackModeName(int ackMode) {
        switch(ackMode)
        {
            case Session.DUPS_OK_ACKNOWLEDGE:      
                return "DUPS_OK_ACKNOWLEDGE";
            case Session.AUTO_ACKNOWLEDGE:         
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:       
                return "CLIENT_ACKNOWLEDGE";
            case jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE:         
                return "EXPLICIT_CLIENT_ACKNOWLEDGE";
            case jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE:         
                return "EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE";
            case jmsProviderSpecifics.TIBCO_NO_ACKNOWLEDGE:     
                return "NO_ACKNOWLEDGE";
            default:                                         
                return "(unknown)";
        }
    }


    protected Flavor getFlavor(String[] args)
    {
        int i=0;

        for (i=0; i < args.length; i++)
        {
            if (args[i].compareTo("-flavor")==0)
            {
                if ((i+1) >= args.length)
                {
                	System.err.print("Flavor must be one of : ");
                	Flavor[] vals = Flavor.values();
                	for (int k=0; k<vals.length; k++){
                		System.err.print(vals[i].toString()+" ");
                	}
                	System.err.println();
                	System.exit(-1);
                }
                flavor = Flavor.valueOf(args[i+1]);
                break;
            }
        }
        return flavor;
    }
    protected String getPropFile(String[] args)
    {
        String filename = null;
        int i=0;
        for (i=0; i < args.length; i++)
        {
            if (args[i].compareToIgnoreCase("-propFile")==0)
            {
                if ((i+1) >= args.length)
                {
                	System.err.print("Must specify filename with -propFile options.");
                	System.exit(-1);
                }
                filename = args[i+1];
                break;
            }
        }
        return filename;
    }
    

    public void createConnectionFactoryAndConnections() throws NamingException, JMSException
    {
        // lookup the connection factory
        ConnectionFactory factory = null;
        if (factoryName != null)
        {
        	jmsUtilities.initJNDI(props);
            factory = (ConnectionFactory) jmsUtilities.lookup(factoryName);
        }
//        else 
//        {
//            factory = new com.tibco.tibjms.TibjmsConnectionFactory(jndiProviderURL);
//        }
        
        // create the connections
        connsVector = new Vector(connections);
        for (int i=0;i<connections;i++)
        {
            Connection conn = factory.createConnection(username, password);
            conn.start();
            connsVector.add(conn);
        }
        System.err.println(connsVector.size() + " connections created.");
    }
        
    public void createXAConnectionFactoryAndXAConnections() throws NamingException, JMSException
    {
        // lookup the connection factory
        XAConnectionFactory factory = null;
        if (factoryName != null)
        {
            jmsUtilities.initJNDI(props);
            factory = (XAConnectionFactory) jmsUtilities.lookup(factoryName);
        }
//        else 
//        {
//            factory = new com.tibco.tibjms.TibjmsXAConnectionFactory(jndiProviderURL);
//        }
        
        // create the connections
        connsVector = new Vector(connections);
        for (int i=0;i<connections;i++)
        {
            XAConnection conn = factory.createXAConnection(username,password);
            conn.start();
            connsVector.add(conn);
        }
    }

    public void cleanup() throws JMSException
    {
        // close the connections
        for (int i=0;i<this.connections;i++) 
        {
            if (!xa)
            {
                Connection conn = (Connection)connsVector.elementAt(i);
                conn.close();
            }
            else
            {
                XAConnection conn = (XAConnection)connsVector.elementAt(i);
                conn.close();
            }
        }
    }

    /**
     * Returns a connection, synchronized because of multiple prod/cons threads
     */
    public synchronized Connection getConnection()
    {
        Connection connection = (Connection)connsVector.elementAt(connIter++);
        if (connIter == connections)
            connIter = 0;
        return connection;
    }

    /**
     * Returns a connection, synchronized because of multiple prod/cons threads
     */
    public synchronized XAConnection getXAConnection()
    {
        XAConnection connection = (XAConnection)connsVector.elementAt(connIter++);
        if (connIter == connections)
            connIter = 0;
        return connection;
    }

    /**
     * Returns a destination, synchronized because of multiple prod/cons threads
     */
    public synchronized Destination getDestination(Session s) throws JMSException
    {
        if (destType.compareToIgnoreCase(DESTTYPE_TOPIC)==0)
        {
            if (!uniqueDests)
                return s.createTopic(destName);
            else
                return s.createTopic(destName + "." + ++destIter);
        }
        else
        {
            if (!uniqueDests)
                return s.createQueue(destName);
            else
                return s.createQueue(destName + "." + ++destIter);
        }
    }

    /**
     * Returns a unique subscription name if durable
     * subscriptions are specified, synchronized because of multiple prod/cons threads
     */
    public synchronized String getSubscriptionName()
    {
        if (durableName != null)
            return durableName + ++nameIter;
        else
            return null;
    }

    /**
     * Returns a txn helper object for beginning/commiting transaction
     * synchronized because of multiple prod/cons threads
     */
    public synchronized tibjmsPerfTxnHelper getPerfTxnHelper(boolean xa)
    {
        return new tibjmsPerfTxnHelper(xa);
    }
    
    /**
     * Helper class for beginning/commiting transactions, maintains
     * any requried state. Each prod/cons thread needs to get an instance of 
     * this by calling getPerfTxnHelper().
     */
    public class tibjmsPerfTxnHelper
    {
        public boolean startNewXATxn = true;
        public Xid     xid = null;
        public boolean xa = false;

        public tibjmsPerfTxnHelper(boolean xa) { 
            this.xa = xa;
        }

        public void beginTx(XAResource xaResource) throws JMSException
        {
            if (xa && startNewXATxn)
            {
                /* create a transaction id */
                java.rmi.server.UID uid = new java.rmi.server.UID();
//                this.xid = new com.tibco.tibjms.TibjmsXid(0, uid.toString(), "branch");
                
                /* start a transaction */
                try {
                    xaResource.start(xid, XAResource.TMNOFLAGS);
                } catch (XAException e) {
                    System.err.println("XAException: " + " errorCode=" + e.errorCode);
                    e.printStackTrace();
                    System.exit(0);
                }
                startNewXATxn = false;
            }
        }
        
        public void commitTx(XAResource xaResource, Session session) throws JMSException
        {
            if (xa)
            {
                if (xaResource != null && xid != null)
                {
                    /* end and prepare the transaction */
                    try {
                        xaResource.end(xid, XAResource.TMSUCCESS);
                        xaResource.prepare(xid);
                    }
                    catch (XAException e) 
                    {
                        System.err.println("XAException: " + " errorCode=" + e.errorCode);
                        e.printStackTrace();
                        
                        Throwable cause = e.getCause();
                        if (cause != null)
                        {
                            System.err.println("cause: ");
                            cause.printStackTrace();
                        }
                        
                        try { 
                            xaResource.rollback(xid); 
                        } catch (XAException re) {}
                        
                        System.exit(0);
                    }
                    
                    /* commit the transaction */
                    try {
                        xaResource.commit(xid, false);
                    } 
                    catch (XAException e) 
                    {
                        System.err.println("XAException: " + " errorCode=" + e.errorCode);
                        e.printStackTrace();
                        
                        Throwable cause = e.getCause();
                        if (cause != null)
                        {
                            System.err.println("cause: ");
                            cause.printStackTrace();
                        }
                        
                        System.exit(0);
                    }
                    startNewXATxn = true;
                    xid = null;
                }
            }
            else
            {
                session.commit();
            }
        }
    }
}

