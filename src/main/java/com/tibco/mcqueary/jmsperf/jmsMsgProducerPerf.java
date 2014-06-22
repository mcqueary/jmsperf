package com.tibco.mcqueary.jmsperf;
/* 
 * Copyright (c) 2001-$Date: 2009-09-11 14:08:04 -0700 (Fri, 11 Sep 2009) $ TIBCO Software Inc. 
 * All rights reserved.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: jmsMsgProducerPerf.java 41997 2009-09-11 21:08:04Z bmahurka $
 * 
 */

/*
 * This is a sample message producer class used to measure performance.
 *
 * For the the specified number of threads this sample creates a 
 * session and a message producer for the specified destination.
 * Once the specified number of messages are produced the performance
 * results are printed and the program exits.
 *
 * Usage:  java jmsMsgProducerPerf  [options]
 *
 *  where options are:
 *
 *   -jndi         <url>         JNDI Provider URL. Default is null.
 *   -user         <username>    User name. Default is null.
 *   -password     <password>    User password. Default is null.
 *   -topic        <topic-name>  Topic name. Default is "topic.sample".
 *   -queue        <queue-name>  Queue name. No default.
 *   -size         <num bytes>   Message payload size in bytes. Default is 100.
 *   -count        <num msgs>    Number of messages to send. Default is 10k.
 *   -time         <seconds>     Number of seconds to run. Default is 0 (forever).
 *   -delivery     <mode>        Delivery mode. Default is NON_PERSISTENT.
 *                               Other values: PERSISTENT and RELIABLE.
 *   -threads      <num threads> Number of message producer threads. Default is 1.
 *   -connections  <num conns>   Number of message producer connections. Default is 1.
 *   -txnsize      <num msgs>    Number of nessages per producer transaction. Default is 0.
 *   -rate         <msg/sec>     Message rate for producer threads.
 *   -payload      <file name>   File containing message payload.
 *   -factory      <lookup name> Lookup name for connection factory.
 *   -uniquedests                Each producer thread uses a unique destination.
 *   -compression                Enable message compression.
 *   -xa                         Use XA transactions.
 */

import java.io.*;
import java.util.*;
import javax.jms.*;
import javax.naming.*;
import javax.transaction.xa.*;

public class jmsMsgProducerPerf 
    extends jmsPerfCommon
    implements Runnable 
{
    // parameters
    private String payloadFile = null;
    private boolean compression = false;
    private int msgRate = 0;
    private int txnSize = 0;
    private int count = 10000;
    private int runTime = 0;
    private int minMsgSize = 0;
    private int maxMsgSize = 0;
    private int threads = 1;
    private int deliveryMode = DeliveryMode.NON_PERSISTENT;
    private StringBuffer msgBuffer = null;
    private byte[] payloadBytes = null;

    // variables
    private int sentCount;
    private long startTime;
    private long endTime;
    private long elapsed;
    private boolean stopNow;

    private boolean useRandomSize = false;
    private Random rand = null;

    // create the producer threads
    /**
     * Constructor
     * 
     * @param args the command line arguments
     */
    public jmsMsgProducerPerf(String[] args)
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

        try {
            jmsUtilities.initSSLParams(jndiProviderURL,args);

            // print parameters
            System.err.println();
            System.err.println("------------------------------------------------------------------------");
            System.err.println("jmsMsgProducerPerf");
            System.err.println("------------------------------------------------------------------------");
            if (propFileName != null) 	           	
            	System.err.println("Property File................ " + propFileName);
            System.err.println("Broker Flavor................ " + flavor);
            System.err.println("JNDI Provider................ " + jndiProviderURL);
            System.err.println("Connection Factory........... " + factoryName);
            System.err.println("User......................... " + username);
            System.err.println("Destination.................. " + destName);
            if (useRandomSize)
            {
                System.err.println("Min Message Size............. " + minMsgSize);            	
                System.err.println("Max Message Size............. " + (payloadFile != null ? payloadFile : String.valueOf(maxMsgSize)));
            }
            else
            {
            	System.err.println("Message Size................. " + (payloadFile != null ? payloadFile : String.valueOf(maxMsgSize)));
            }
            if (count > 0)
                System.err.println("Count........................ " + count);
            if (runTime > 0)
                System.err.println("Duration..................... " + runTime);
            System.err.println("Production Threads........... " + threads);
            System.err.println("Production Connections....... " + connections);
            System.err.println("Unique Destinations.......... " + uniqueDests);
            System.err.println("DeliveryMode................. " + deliveryModeName(deliveryMode));
            System.err.println("Compression.................. " + compression);
            System.err.println("XA........................... " + xa);
            if (msgRate > 0)
                System.err.println("Message Rate................. " + msgRate);
            if (txnSize > 0)
                System.err.println("Transaction Size............. " + txnSize);
            System.err.println("------------------------------------------------------------------------");
            System.err.println();

            if (!xa)
                createConnectionFactoryAndConnections();
            else
                createXAConnectionFactoryAndXAConnections();

            // create the producer threads
            Vector<Thread> tv = new Vector<Thread>(threads);
            for (int i=0;i<threads;i++)
            {
                Thread t = new Thread(this);
                tv.add(t);
                t.start();
            }

            // run for the specified amount of time
            if (runTime > 0)
            {
                try 
                {
                    Thread.sleep(runTime * 1000);
                } 
                catch (InterruptedException e) {}

                // ensure producer threads stop now
                stopNow = true;
                for (int i=0;i<threads;i++)
                {
                    Thread t = (Thread)tv.elementAt(i);
                    t.interrupt();
                }
            }

            // wait for the producer threads to exit
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
     * Update the total sent count.
     */
    private synchronized void countSends(int count)
    {
        sentCount += count;
    }

    /** 
     * The producer thread's run method.
     */
    public void run()
    {
        int msgCount = 0;
        MsgRateChecker msgRateChecker = null;
        
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {}

        try {
            MessageProducer     msgProducer = null;
            Destination         destination = null;
            Session             session     = null;
            XAResource          xaResource  = null;
            jmsPerfTxnHelper txnHelper   = getPerfTxnHelper(xa);

            if (!xa)
            {
                // get the connection
                Connection connection = getConnection();
                // create a session
                session = connection.createSession(txnSize > 0, Session.AUTO_ACKNOWLEDGE);
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
                
            // create the producer
            msgProducer = session.createProducer(destination);

            // set the delivery mode
            msgProducer.setDeliveryMode(deliveryMode);
            
            // performance settings
            msgProducer.setDisableMessageID(true);
            msgProducer.setDisableMessageTimestamp(true);

            // create the message
            Message msg = createMessage(session);

            // enable compression if necessary
            if (compression)
                msg.setBooleanProperty("JMS_TIBCO_COMPRESS", true); 

            // initialize message rate checking
            if (msgRate > 0)
                msgRateChecker = new MsgRateChecker(msgRate);

            startTiming();
            
            int randomSize = 0;
            
            // publish messages
            while ((count == 0 || msgCount < (count/threads)) && !stopNow)
            {
                // a no-op for local txns
                txnHelper.beginTx(xaResource);

                if (useRandomSize)
                {
                	if (rand==null)
                    	rand = new Random();
                	randomSize = rand.nextInt(maxMsgSize - minMsgSize + 1) + minMsgSize;
                	//System.out.println("Generating message body of size " + randomSize + " bytes.");
                	msg.clearBody();
                	try {
                		((BytesMessage)msg).writeBytes(payloadBytes,0,randomSize);
                	} catch (IndexOutOfBoundsException e)
                	{
                		System.err.println("Caught IndexOutOfBoundsException while writng message body:");
                		System.err.println("	payloadBytes.length == " + payloadBytes.length);
                		System.err.println("	randomSize == " + randomSize);
                	}
                }
               	msgProducer.send(msg);
                
                msgCount++;

                // commit the transaction if necessary
                if (txnSize > 0 && msgCount % txnSize == 0)
                    txnHelper.commitTx(xaResource, session);
                
                // check the message rate
                if (msgRate > 0)
                    msgRateChecker.checkMsgRate(msgCount);
            }
            
            // commit any remaining messages
            if (txnSize > 0)
                txnHelper.commitTx(xaResource, session);
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

        stopTiming();
        
        countSends(msgCount);
    }

    /**
     * Create the message.
     */
    private Message createMessage(Session session) throws JMSException
    {
        String payload = null;
        int bufferSize = 0;
        // create the message
        BytesMessage msg = session.createBytesMessage();
        
        // add the payload
        if (payloadFile != null)
        {
            try
            {
                InputStream instream = 
                    new BufferedInputStream(new FileInputStream(payloadFile));
                bufferSize = instream.available();
                byte[] bytesRead = new byte[bufferSize];
                instream.read(bytesRead);
                instream.close();

                payload = new String(bytesRead);
                
                if (minMsgSize > bufferSize)
                {
                	System.err.println("Payload file size (" + bufferSize + ") < minimum msg size (" 
                			+ maxMsgSize + ")");
                	System.err.println("Exiting."); 
                	System.exit(-1);
                }
                
                if (maxMsgSize > bufferSize)
                {
                	System.err.println("Payload file size (" + bufferSize + ") < maximum msg size (" 
                			+ maxMsgSize + "). Setting maximum msg size to " + bufferSize);
                	maxMsgSize = bufferSize;
                }

            }
            catch(IOException e)
            {
                System.err.println("Error: unable to load payload file - " + e.getMessage());
            }
            
        }
        
        
        if (maxMsgSize > 0)
        {
            msgBuffer = new StringBuffer(maxMsgSize);
            char c = 'A';
            for (int i = 0; i < maxMsgSize; i++)
            {
                msgBuffer.append(c++);
                if (c > 'z')
                    c = 'A';
            }
            payload = msgBuffer.toString();
        }
        
        if (payload != null)
        {
        	payloadBytes = payload.getBytes();
            // add the payload to the message
            msg.writeBytes(payloadBytes);
        }
        
        return msg;
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
     * Convert delivery mode to a string.
     */
    private String deliveryModeName(int mode) {
        switch(mode)
        {
            case javax.jms.DeliveryMode.PERSISTENT:         
                return "PERSISTENT";
            case javax.jms.DeliveryMode.NON_PERSISTENT:     
                return "NON_PERSISTENT";
            case jmsProviderSpecifics.TIBCO_RELIABLE_DELIVERY: 
                return "RELIABLE";
            default:                                        
                return "(unknown)";
        }
    }

    private int deliveryModeNum(String name)
    {
    	int mode = -9999;
        if (name.compareTo("PERSISTENT")==0)
            mode = javax.jms.DeliveryMode.PERSISTENT;
        else if (name.compareTo("NON_PERSISTENT")==0)
            mode = javax.jms.DeliveryMode.NON_PERSISTENT;
        else if ((flavor==Flavor.TIBEMS) && (name.compareTo("RELIABLE")==0))
            mode = jmsProviderSpecifics.TIBCO_RELIABLE_DELIVERY;

        return mode;
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
            int perf = (int)((sentCount * 1000.0)/elapsed);
            return (sentCount + " times took " + seconds + " seconds, performance is " + perf + " messages/second");
        }
        else
        {
            return "interval too short to calculate a message rate";
        }
    }

    /**
     * Print the usage and exit.
     */
    private void usage()
    {
        System.err.println("\nUsage: java jmsMsgProducerPerf [options] [ssl options]");
        System.err.println("\n");
        System.err.println("   where options are:");
        System.err.println("");
        System.err.println("   -propFile     <filename>      - Properties file");
        System.err.print("   -flavor       <broker type>   - Broker flavor (");
        Flavor[] flavs = Flavor.values();
        for (int i=0; i < flavs.length; i++)
        {
        	System.err.print(flavs[i].name());
        	if (i+1 < flavs.length)
        		System.err.print(",");
        	else
        		System.err.println(")");
        }
        System.err.println("   -jndi         <URL>         - JNDI Provider URL, default is null");
        System.err.println("   -user         <user name>   - user name, default is null");
        System.err.println("   -password     <password>    - password, default is null");
        System.err.println("   -topic        <topic-name>  - topic name, default is \"topic.sample\"");
        System.err.println("   -queue        <queue-name>  - queue name, no default");
        System.err.println("   -size         <nnnn>        - Message payload in bytes");
        System.err.println("   -size    rand <min> <max>   - Random message size in specified range (bytes)");
        System.err.println("   -count        <nnnn>        - Number of messages to send, default 10k");
        System.err.println("   -time         <seconds>     - Number of seconds to run");
        System.err.println("   -threads      <nnnn>        - Number of threads to use for sends");
        System.err.println("   -connections  <nnnn>        - Number of connections to use for sends");
        System.err.println("   -delivery     <nnnn>        - DeliveryMode, default NON_PERSISTENT");
        System.err.println("   -txnsize      <count>       - Number of messages per transaction");
        System.err.println("   -rate         <msg/sec>     - Message rate for each producer thread");
        System.err.println("   -payload      <file name>   - File containing message payload.");
        System.err.println("   -factory      <lookup name> - Lookup name for connection factory.");
        System.err.println("   -uniquedests                - Each producer uses a different destination");
        System.err.println("   -compression                - Enable compression while sending msgs ");
        System.err.println("   -xa                         - Use XA transactions ");
        System.err.println("   -help-ssl                   - help on ssl parameters\n");
        
        System.exit(0);
    }

    void initParams()
    {
        debug = Boolean.parseBoolean(props.getProperty(OPT_DEBUG, "false"));
        flavor = Flavor.valueOf(props.getProperty(OPT_PROVIDER_FLAVOR));
        connections = Integer.parseInt(props.getProperty(OPT_PRODUCER_CONNECTIONS, "1"));
        threads = Integer.parseInt(props.getProperty(OPT_PRODUCER_THREADS, "1"));
        jndiProviderURL = props.getProperty(OPT_PROVIDER_JNDI, null);
        username = props.getProperty(OPT_USERNAME, null);
        password = props.getProperty(OPT_PASSWORD, null);
        destType = props.getProperty(OPT_DESTINATION_TYPE, "topic");
        destName = props.getProperty(OPT_DESTINATION_NAME, "topic.sample");
        if (destType.equalsIgnoreCase("topic")) {
        	destNameFormat = props.getProperty(jmsPerfCommon.OPT_DEST_NAME_FORMAT_TOPIC, "%s");        	
        }
        else
        	destNameFormat = props.getProperty(jmsPerfCommon.OPT_DEST_NAME_FORMAT_QUEUE, "%s");
        factoryName = props.getProperty(OPT_FACTORY, null);
        uniqueDests = Boolean.parseBoolean(props.getProperty(OPT_UNIQUE_DESTS, "false"));
        xa = Boolean.parseBoolean(props.getProperty(OPT_USE_XA, "false"));
    	
    	
        payloadFile = props.getProperty(OPT_PRODUCER_PAYLOAD_FILE, null);
        compression = Boolean.parseBoolean(props.getProperty(OPT_COMPRESSION, "false"));
        msgRate = Integer.parseInt(props.getProperty(OPT_PRODUCER_MESSAGE_RATE, "0"));
        txnSize = Integer.parseInt(props.getProperty(OPT_TXNSIZE, "0"));
        count = Integer.parseInt(props.getProperty(OPT_COUNT, "10000"));
        runTime = Integer.parseInt(props.getProperty(OPT_DURATION, "0"));
        minMsgSize = Integer.parseInt(props.getProperty(OPT_PRODUCER_PAYLOAD_MINSIZE, "0"));
        maxMsgSize = Integer.parseInt(props.getProperty(OPT_PRODUCER_PAYLOAD_MAXSIZE, "0"));
        if (minMsgSize < maxMsgSize)
        	useRandomSize = true;
        String deliveryModeString = props.getProperty(OPT_PRODUCER_DELIVERY_MODE,
        		"NON_PERSISTENT");
        deliveryMode = deliveryModeNum(deliveryModeString);
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
        		//skip it -- already processed
        		i+=2;
        	}
        	else if (args[i].compareTo("-server")==0)
            {
                if ((i+1) >= args.length) usage();
                jndiProviderURL = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-queue")==0)
            {
                if ((i+1) >= args.length) usage();
                destName = args[i+1];
                i += 2;
                destType = DESTTYPE_QUEUE;
            }
            else if (args[i].compareTo("-topic")==0)
            {
                if ((i+1) >= args.length) usage();
                destName = args[i+1];
                i += 2;
                destType = DESTTYPE_TOPIC;
            }
            else if (args[i].compareTo("-user")==0)
            {
                if ((i+1) >= args.length) usage();
                username = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-password")==0)
            {
                if ((i+1) >= args.length) usage();
                password = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-delivery")==0)
            {
                if ((i+1) >= args.length) usage();
                String dm = args[i+1];
                i += 2;
                if (dm.compareTo("PERSISTENT")==0)
                    deliveryMode = javax.jms.DeliveryMode.PERSISTENT;
                else if (dm.compareTo("NON_PERSISTENT")==0)
                    deliveryMode = javax.jms.DeliveryMode.NON_PERSISTENT;
                else if (dm.compareTo("RELIABLE")==0)
                    deliveryMode = jmsProviderSpecifics.TIBCO_RELIABLE_DELIVERY;
                else {
                    System.err.println("Error: invalid value of -delivery parameter");
                    usage();
                }
            }
            else if (args[i].compareTo("-count")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    count = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) {
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
                catch(NumberFormatException e) {
                    System.err.println("Error: invalid value of -time parameter");
                    usage();
                }
                i += 2;
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
                catch(NumberFormatException e) {
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
            else if (args[i].compareTo("-size")==0)
            {
                if ((i+1) >= args.length) usage();
                if (args[i+1].compareTo("rand")==0)
                {
                	useRandomSize = true;
                    if ((i+2) >= args.length) usage();
                	try {
                		minMsgSize = Integer.parseInt(args[i+2]);
                	}
                    catch(NumberFormatException e) 
                    {
                        System.err.println("Error: invalid value of -size rand parameter");
                        usage();
                    }
                    i += 2;
                }
                try 
                {
                    maxMsgSize = Integer.parseInt(args[i+1]);
                }
                catch(NumberFormatException e) 
                {
                    System.err.println("Error: invalid value of -size parameter");
                    usage();
                }
                i += 2;
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
            else if (args[i].compareTo("-rate")==0)
            {
                if ((i+1) >= args.length) usage();
                try 
                {
                    msgRate = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e)
                {
                    System.err.println("Error: invalid value of -rate parameter");
                    usage();
                }
                if (msgRate < 1)
                {
                    System.err.println("Error: invalid value of -rate parameter");
                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-payload")==0)
            {
                if ((i+1) >= args.length) usage();
                payloadFile = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-factory")==0)
            {
                if ((i+1) >= args.length) usage();
                factoryName = args[i+1];
                i += 2;
            }
            else if (args[i].startsWith("-ssl"))
            {
                i += 2;
            }
            else if (args[i].compareTo("-uniquedests")==0)
            {
                uniqueDests = true;
                i += 1;
            }
            else if (args[i].compareTo("-compression")==0)
            {
                compression = true;
                i += 1;
            } 
            else if (args[i].compareTo("-xa")==0)
            {
                xa = true;
                i += 1;
            } 
            else if (args[i].compareTo("-help")==0)
            {
                usage();
            }
            else
            {
                System.err.println("Error: invalid option: " + args[i]);
                usage();
            }
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
     * Get the total produced message count.
     */
    public int getSentCount()
    {
        return sentCount;
    }

    /**
     * Class used to control the producer's send rate.
     */
    private class MsgRateChecker 
    {
        long sampleStart;
        int sampleTime;
        int sampleCount;
        
        MsgRateChecker(int rate)
        {
            // initialize
            this.sampleTime = 10;
        }
        
        void checkMsgRate(int count)
        {
            if (msgRate < 100)
            {
                if (count % 10 == 0)
                {
                   try {
                       long sleepTime = (long)((10.0/(double)msgRate)*1000);
                       Thread.sleep(sleepTime);
                   } catch(InterruptedException e) {}
                }
            }
            else if (sampleStart == 0)
            {
                sampleStart = System.currentTimeMillis();
            }
            else
            {
                long elapsed = System.currentTimeMillis() - sampleStart;
                if (elapsed >= sampleTime)
                {
                    int actualMsgs = count - sampleCount;
                    int expectedMsgs = (int)(elapsed*((double)msgRate/1000.0));
                    if (actualMsgs > expectedMsgs)
                    {
                        long sleepTime = (long)((double)(actualMsgs-expectedMsgs)/((double)msgRate/1000.0));
                        try 
                        {
                            Thread.sleep(sleepTime);
                        }
                        catch (InterruptedException e) {}
                        if (sampleTime > 20)
                            sampleTime -= 10;
                    }
                    else
                    {
                        if (sampleTime < 300)
                            sampleTime += 10;
                    }
                    sampleStart = System.currentTimeMillis();
                    sampleCount = count;
                }
            }
        }
    }
    
    /**
     * main
     */
    public static void main(String[] args)
    {
        new jmsMsgProducerPerf(args);
    }
}
