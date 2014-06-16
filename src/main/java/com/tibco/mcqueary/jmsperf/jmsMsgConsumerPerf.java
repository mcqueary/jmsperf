package com.tibco.mcqueary.jmsperf;
/* 
 * Copyright (c) 2001-$Date: 2010-07-07 10:49:24 -0700 (Wed, 07 Jul 2010) $ TIBCO Software Inc. 
 * All rights reserved.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: jmsMsgConsumerPerf.java 47621 2010-07-07 17:49:24Z bmahurka $
 * 
 */

/*
 * This is a sample message consumer class used to measure performance.
 * 
 * For the the specified number of threads this sample creates a 
 * session and a message consumer for the specified destination.
 * Once the specified number of messages are consumed the performance
 * results are printed and the program exits.
 *
 * Usage:  java jmsMsgConsumerPerf  [options]
 *
 *  where options are:
 *
 *   -jndiUrl       <url>        JNDI provider URL. Default is null.
 *   -user         <username>    User name. Default is null.
 *   -password     <password>    User password. Default is null.
 *   -topic        <topic-name>  Topic name. Default is "topic.sample".
 *   -queue        <queue-name>  Queue name. No default.
 *   -count        <num msgs>    Number of messages to consume. Default is 10k.
 *   -time         <seconds>     Number of seconds to run. Default is 0 (forever).
 *   -threads      <num threads> Number of message consumer threads. Default is 1.
 *   -connections  <num conns>   Number of message consumer connections. Default is 1.
 *   -txnsize      <num msgs>    Number of messages per consumer transaction. Default is 0.
 *   -durable      <name>        Durable subscription name.
 *   -selector     <selector>    Message selector for consumer threads. No default.
 *   -ackmode      <mode>        Message acknowledge mode. Default is AUTO.
 *                               Other values: DUPS_OK, CLIENT EXPLICIT_CLIENT,
 *                               EXPLICIT_CLIENT_DUPS_OK and NO.
 *   -factory      <lookup name> Lookup name for connection factory.
 *   -uniquedests                Each consumer thread uses a unique destination.
 *   -xa                         Use XA transactions.
 */

import java.util.*;

import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

public class jmsMsgConsumerPerf 
    extends jmsPerfCommon
//    implements Runnable, com.tibco.tibjms.TibjmsMulticastExceptionListener
    implements Runnable
{
    // parameters
    private String selector = null;
    private int txnSize = 0;
    private int count = 10000;
    private int runTime = 0;
    private int threads = 1;
    private int ackMode = Session.AUTO_ACKNOWLEDGE;

    // variables
    private int recvCount;
    private long startTime;
    private long endTime;
    private long elapsed;
    private boolean stopNow;

    /**
     * Constructor
     * 
     * @param args the command line arguments
     */
    public jmsMsgConsumerPerf(String[] args)
    {
    	String propFileName = getPropFile(args);
    	
    	// Get provider flavor (specified by the -flavor command line arg
//    	flavor = getFlavor(args);
    	
    	if (propFileName != null)
    	{
    		// Initialize from properties file if it exists
            initProperties(propFileName);            
        	initParams();
    	}

    	// Override the properties with command line args if they exist
    	parseArgs(args);

        jmsUtilities.initJNDI(props);
        
        try {
            jmsUtilities.initSSLParams(jndiProviderURL,args);

            // print parameters
            System.err.println();
            System.err.println("------------------------------------------------------------------------");
            System.err.println("jmsMsgConsumerPerf");
            System.err.println("------------------------------------------------------------------------");
            if (propFileName != null) 	           	
            	System.err.println("Property File................ " + propFileName);
            System.err.println("Broker Flavor................ " + flavor);
            System.err.println("JNDI Provider................ " + jndiProviderURL);
            System.err.println("Connection Factory........... " + factoryName);
            System.err.println("User......................... " + username);
            System.err.println("Destination.................. " + destName);
            System.err.println("Count........................ " + count);            
            System.err.println("Consumer Threads............. " + threads);
            System.err.println("Consumer Connections......... " + connections);
            System.err.println("Unique Destinations.......... " + uniqueDests);
            System.err.println("Ack Mode..................... " + ackModeName(ackMode));
            System.err.println("Durable...................... " + (durableName != null));
            System.err.println("Selector..................... " + selector);
            System.err.println("XA........................... " + xa);
            if (txnSize > 0)
                System.err.println("Transaction Size............. " + txnSize);
            System.err.println("------------------------------------------------------------------------");
            System.err.println();

            if (!xa)
                createConnectionFactoryAndConnections();
            else
                createXAConnectionFactoryAndXAConnections();
            
            // create the consumer threads
            Vector tv = new Vector(threads);
            for (int i=0;i<threads;i++)
            {
                Thread t = new Thread(this);
                tv.add(t);
                t.start();
            }

            // run for the specified amout of time
            if (runTime > 0)
            {
                try 
                {
                    Thread.sleep(runTime * 1000);
                } 
                catch (InterruptedException e) {}

                // ensure consumer threads stop now
                stopNow = true;
                for (int i=0;i<threads;i++)
                {
                    Thread t = (Thread)tv.elementAt(i);
                    t.interrupt();
                }
            }

            // wait for the consumer threads to exit
            for (int i=0;i<threads;i++)
            {
                Thread t = (Thread)tv.elementAt(i);
                try 
                {
                    t.join();
                } 
                catch (InterruptedException e) {}
            }

            // close connections
            cleanup();

            // print performance
            System.err.println(getPerformance());
        }
        catch (NamingException e)
        {
            e.printStackTrace();
        }
        catch (JMSException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Update the total receive count.
     */
    private synchronized void countReceives(int count)
    {
        recvCount += count;
    }

    /**
     * The consumer thread's run method.
     */
    public void run()
    {
        Session             session          = null;
        MessageConsumer     msgConsumer      = null;
        String              subscriptionName = getSubscriptionName();
        int                 msgCount         = 0;
        Destination         destination      = null;
        XAResource          xaResource       = null;
        tibjmsPerfTxnHelper txnHelper        = getPerfTxnHelper(xa);

        try 
        {
            Thread.sleep(250);
        } catch (InterruptedException e) {}

        try {

            if (!xa)
            {
                // get the connection
                Connection connection = getConnection();
                // create a session
                session = connection.createSession(txnSize > 0, ackMode);
            }
            else
            {
                // get the connection
                XAConnection connection = getXAConnection();            
                // create a session
                session = connection.createXASession();
            }

            if (xa)
                /* get the XAResource for the XASession */
                xaResource = ((javax.jms.XASession)session).getXAResource();

            // get the destination
            destination = getDestination(session);
//System.err.println("Got destination " + destination);            
            // create the consumer
            if (subscriptionName == null)
                msgConsumer = session.createConsumer(destination, selector);
            else
                msgConsumer = session.createDurableSubscriber((Topic)destination,
                                                              subscriptionName,
                                                              selector,
                                                              false);

            // register multicast exception listener for multicast consumers
//            if (com.tibco.tibjms.Tibjms.isConsumerMulticast(msgConsumer))
//                com.tibco.tibjms.Tibjms.setMulticastExceptionListener(this);

            boolean startNewXATxn = true;

            // receive messages
            while ((count == 0 || msgCount < (count/threads)) && !stopNow)
            {
                // a no-op for local txns
                txnHelper.beginTx(xaResource);

                Message msg = msgConsumer.receive();
                if (msg == null)
                    break;

                if (msgCount == 0)
                    startTiming();

                msgCount++;

                // acknowledge the message if necessary

                if ((flavor != null) && (flavor == Flavor.TIBEMS))
                {
                	if ((ackMode == jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE) ||
                    (ackMode == jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE))
                    {
                    	msg.acknowledge();
                    }
                }
                else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
                    msg.acknowledge();

                // commit the transaction if necessary
                if (txnSize > 0 && msgCount % txnSize == 0)
                    txnHelper.commitTx(xaResource, session);
                
                if ((flavor != null) && (flavor == Flavor.TIBEMS))
                {
	                // force the uncompression of compressed messages
	                if (msg.getBooleanProperty("JMS_TIBCO_COMPRESS"))
	                    ((BytesMessage) msg).getBodyLength();
                }
            }
        }
        catch (JMSException e)
        {
            if (!stopNow)
            {
                System.err.println("exception: ");
                e.printStackTrace();

                Exception le = e.getLinkedException();
                if (le != null)
                {
                    System.err.println("linked exception: ");
                    le.printStackTrace();
                }
            }
        }

        // commit any remaining messages
        if (txnSize > 0)
        {
            try 
            {
                txnHelper.commitTx(xaResource, session);
            }
            catch (JMSException e) 
            {
                if (!stopNow)
                    e.printStackTrace();
            }
        }
        
        stopTiming();

        countReceives(msgCount);

        try 
        {
            if (msgConsumer != null)
                msgConsumer.close();
            
            // unsubscribe durable subscription
            if (subscriptionName != null) 
            {
                if (session != null)
                    session.unsubscribe(subscriptionName);
            }

            session.close();
        }
        catch (JMSException e) 
        {
            e.printStackTrace();
        }
    }

    private synchronized void startTiming()
    {
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }
    
    private synchronized void stopTiming()
    {
        endTime = System.currentTimeMillis();
    }
    
    /**
     * Get the performance results.
     */
    private String getPerformance() 
    {
        if (endTime > startTime)
        {
            elapsed = endTime - startTime;
            double seconds = elapsed/1000.0;
            int perf = (int)((recvCount * 1000.0)/elapsed);
            return (recvCount + " times took " + seconds + " seconds, performance is " + perf + " messages/second");
        }
        else
        {
            return "interval too short to calculate a message rate";
        }
    }

    /**
     * Print usage and exit.
     */
    private void usage()
    {
        System.err.println();
        System.err.println("Usage: java jmsMsgConsumerPerf [options] [ssl options]");
        System.err.println();
        System.err.println("  where options are:");
        System.err.println();
        System.err.println("    -propFile     <filename>      - Properties file");
        System.err.print("    -flavor       <broker type>   - Broker flavor (");
        Flavor[] flavs = Flavor.values();
        for (int i=0; i < flavs.length; i++)
        {
        	System.err.print(flavs[i].name());
        	if (i+1 < flavs.length)
        		System.err.print(",");
        	else
        		System.err.println(")");
        }
        System.err.println("    -jndi         <url>         - JNDI provider URL. Default is null.");
        System.err.println("    -user         <username>    - User name. Default is null.");
        System.err.println("    -password     <password>    - User password. Default is null.");
        System.err.println("    -topic        <topic-name>  - Topic name. Default is \"topic.sample\".");
        System.err.println("    -queue        <queue-name>  - Queue name. No default.");
        System.err.println("    -count        <num msgs>    - Number of messages to consume. Default is 10k.");
        System.err.println("    -time         <seconds>     - Number of seconds to run. Default is 0.");
        System.err.println("    -threads      <num threads> - Number of consumer threads. Default is 1.");
        System.err.println("    -connections  <num conns>   - Number of consumer connections. Default is 1.");
        System.err.println("    -txnsize      <num msgs>    - Number of messages per consumer transaction.");
        System.err.println("    -durable      <name>        - Durable subscription name. No default.");
        System.err.println("    -selector     <selector>    - Message selector for consumers. No default.");
        System.err.println("    -ackmode      <mode>        - Message acknowledge mode. Default is AUTO.");
        System.err.println("                                  Other values: DUPS_OK, CLIENT EXPLICIT_CLIENT,");
        System.err.println("                                  EXPLICIT_CLIENT_DUPS_OK and NO.");
        System.err.println("    -factory      <lookup name> - Lookup name for connection factory.");
        System.err.println("    -uniquedests                - Each consumer thread uses a unique destination.");
        System.err.println("    -help-ssl                   - Print help on SSL parameters.");
        System.exit(0);
    }

     /**
     * Parse the command line arguments.
     */
    
    private void parseArgs(String[] args)
    {
        int i=0;

        while(i < args.length)
        {
        	if ((args[i].compareTo("-flavor")==0) || (args[i].compareToIgnoreCase("-propFile")==0))
        	{
        		// We already handled this -- skip it
        		i += 2;
        	}
        	else if (args[i].compareTo("-server")==0)
            {
                if ((i+1) >= args.length) usage();
                jndiProviderURL = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-topic")==0)
            {
                if ((i+1) >= args.length) usage();
                destName = args[i+1];
                destType = DESTTYPE_TOPIC;
                i += 2;
            }
            else if (args[i].compareTo("-queue")==0)
            {
                if ((i+1) >= args.length) usage();
                destName = args[i+1];
                destType = DESTTYPE_QUEUE;
                i += 2;
            }
            else if (args[i].compareTo("-durable")==0)
            {
                if ((i+1) >= args.length) usage();
                durableName = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-user")==0)
            {
                if ((i+1) >= args.length) usage();
                username = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-help-ssl")==0)
            {
                jmsUtilities.sslUsage();
            }
            else if (args[i].compareTo("-password")==0)
            {
                if ((i+1) >= args.length) usage();
                password = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-uniquedests")==0)
            {
                uniqueDests = true;
                i += 1;
            }
            else if (args[i].compareTo("-xa")==0)
            {
                xa = true;
                i += 1;
            }
            else if (args[i].compareTo("-threads")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    threads = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -threads parameter");
                    usage();
                }
                if (threads < 1) {
                    System.err.println("Error: invalid value of -threads parameter, must be >= 1");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-connections")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    connections = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -connections parameter");
                    usage();
                }
                if (connections < 1) 
                {
                    System.err.println("Error: invalid value of -connections parameter, must be >= 1");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-count")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    count = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -count parameter");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-time")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    runTime = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -time parameter");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-ackmode")==0)
            {
                if ((i+1) >= args.length) usage();
                String dm = args[i+1];
                i += 2;
                if (dm.compareTo("DUPS_OK")==0)
                    ackMode = javax.jms.Session.DUPS_OK_ACKNOWLEDGE;
                else if (dm.compareTo("AUTO")==0)
                    ackMode = javax.jms.Session.AUTO_ACKNOWLEDGE;
                else if (dm.compareTo("CLIENT")==0)
                    ackMode = javax.jms.Session.CLIENT_ACKNOWLEDGE;
                else if (dm.compareTo("EXPLICIT_CLIENT")==0)
                    ackMode = jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE;
                else if (dm.compareTo("EXPLICIT_CLIENT_DUPS_OK")==0)
                    ackMode = jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE;
                else if (dm.compareTo("NO")==0)
                    ackMode = jmsProviderSpecifics.TIBCO_NO_ACKNOWLEDGE;
                else {
                    System.err.println("Error: invalid value of -ackMode parameter");
                    usage();
                }
            }
            else if (args[i].compareTo("-txnsize")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    txnSize = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -txnsize parameter");
                    usage();
                }
                if (txnSize < 1) 
                {
                    System.err.println("Error: invalid value of -txnsize parameter");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-selector")==0)
            {
                if ((i+1) >= args.length) usage();
                selector = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-factory")==0)
            {
                if ((i+1) >= args.length) usage();
                factoryName = args[i+1];
                i += 2;
            }
            else if(args[i].startsWith("-ssl"))
            {
                i += 2;
            }
            else 
            {
                System.err.println("Error: invalid option: " + args[i]);
                usage();
            }
        }

        if ((durableName != null) && destType.equalsIgnoreCase(DESTTYPE_TOPIC))
        {
            System.err.println("Error: -durable cannot be used with -topic");
            usage();
        }
    }

    /**
     * Get the total elapsed time.
     */
    public long getElapsedTime()
    {
        return elapsed;
    }

    /**
     * Get the total consumed message count.
     */
    public int getReceiveCount()
    {
        return recvCount;
    }

    /**
     * Multicast exception listener
     */
    public void onMulticastException(Connection connection, Session session,
                                     MessageConsumer consumer, JMSException ex)
    {
        System.err.println(ex.getMessage());
        try
        {
            session.close();
        }
        catch (JMSException closeEx)
        {
            // ignore
        }
    }
    void initParams()
    {        
        debug = Boolean.parseBoolean(props.getProperty(DEBUG, "false"));
    	flavor = Flavor.valueOf(props.getProperty(PROVIDER_FLAVOR));
        selector = props.getProperty(CONSUMER_SELECTOR, null);
        txnSize = Integer.parseInt(props.getProperty(TXNSIZE, "0"));
        count = Integer.parseInt(props.getProperty(COUNT, "10000"));
        runTime = Integer.parseInt(props.getProperty(DURATION, "0"));
        threads = Integer.parseInt(props.getProperty(CONSUMER_THREADS, "1"));
        String ackModeString = props.getProperty(CONSUMER_ACK_MODE, "AUTO_ACKNOWLEDGE");
        
        if (ackModeString.compareToIgnoreCase("AUTO_ACKNOWLEDGE")==0)
        	ackMode = Session.AUTO_ACKNOWLEDGE;
        else if (ackModeString.compareToIgnoreCase("CLIENT_ACKNOWLEDGE")==0)
        	ackMode = Session.CLIENT_ACKNOWLEDGE;
        else if (ackModeString.compareToIgnoreCase("DUPS_OK_ACKNOWLEDGE")==0)
        	ackMode = Session.DUPS_OK_ACKNOWLEDGE;
        else if (flavor==Flavor.TIBEMS)
        {
        	if (ackModeString.compareToIgnoreCase("EXPLICIT_ACKNOWLEDGE")==0)
        		ackMode = jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_ACKNOWLEDGE;
        	else if (ackModeString.compareToIgnoreCase("EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE")==0)
        		ackMode = jmsProviderSpecifics.TIBCO_EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE;
        	else if (ackModeString.compareToIgnoreCase("NO_ACKNOWLEDGE")==0)
        		ackMode = jmsProviderSpecifics.TIBCO_NO_ACKNOWLEDGE;
        }
        
        connections = Integer.parseInt(props.getProperty(CONSUMER_CONNECTIONS, "1"));
        jndiProviderURL = props.getProperty(PROVIDER_JNDI, null);
        username = props.getProperty(USERNAME, null);
        password = props.getProperty(PASSWORD, null);
        durableName = props.getProperty(CONSUMER_DURABLE_NAME, null);
        destType = props.getProperty(DESTINATION_TYPE, "topic");
        destName = props.getProperty(DESTINATION_NAME, "topic.sample");
        factoryName = props.getProperty(FACTORY, null);
        uniqueDests = Boolean.parseBoolean(props.getProperty(UNIQUE_DESTS, "false"));
        xa = Boolean.parseBoolean(props.getProperty(USE_XA, "false"));
    }
    /**
     * main
     */
    public static void main(String[] args)
    {
        jmsMsgConsumerPerf t = new jmsMsgConsumerPerf(args);
    }
}
