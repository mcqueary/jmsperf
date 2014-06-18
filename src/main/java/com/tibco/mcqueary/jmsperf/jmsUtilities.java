package com.tibco.mcqueary.jmsperf;

/* 
 * Copyright (c) 2001-$Date: 2009-04-02 14:32:09 -0700 (Thu, 02 Apr 2009) $ TIBCO Software Inc. 
 * All rights reserved.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: jmsUtilities.java 40106 2009-04-02 21:32:09Z rkutter $
 * 
 */

/*
 * This sample uses JNDI to retrieve administered objects.
 *
 * Optionally all parameters hardcoded in this sample can be
 * read from the jndi.properties file.
 *
 * This file also contains an SSL parameter helper class to enable
 * an SSL connection to a TIBCO Enterprise Message Service server.
 *
 */

import javax.jms.*;
import javax.naming.*;

import java.util.Hashtable;
import java.util.Properties;


public class jmsUtilities
{
    static Context jndiContext = null;

    static final String  providerContextFactory = null;

//    static final String  defaultProtocol = "tibjmsnaming";

    static final String  defaultProviderURL = null;

//    public static void initJNDI(String providerURL) throws NamingException
//    {
//        initJNDI(providerURL,null,null);
//    }
    

    public static void initJNDI(Properties props)
    {
    	if (jndiContext != null)
    		return;
		else
		{
			try {
                Hashtable<String,String> env = new Hashtable<String,String>();
                env.put(Context.INITIAL_CONTEXT_FACTORY,props.getProperty(Context.INITIAL_CONTEXT_FACTORY));
                env.put(Context.PROVIDER_URL,props.getProperty(Context.PROVIDER_URL));
                
                String u = props.getProperty(Context.SECURITY_PRINCIPAL, null);
                String p = props.getProperty(Context.SECURITY_CREDENTIALS, null);
                if (u != null) {
                    env.put(Context.SECURITY_PRINCIPAL,u);
                    if (p != null)
                        env.put(Context.SECURITY_CREDENTIALS,p);
                }

                jndiContext = new InitialContext(env);
			} catch (NamingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }

    public static Object lookup(String objectName) throws NamingException
    {
        if (objectName == null)
            throw new IllegalArgumentException("null object name not legal");

        if (objectName.length() == 0)
            throw new IllegalArgumentException("empty object name not legal");

        /*
         * do the lookup
         */
        return jndiContext.lookup(objectName);
    }


    public static void initSSLParams(String jndiProviderURL,String[] args) throws JMSSecurityException{
    	/*
        if (jndiProviderURL != null && jndiProviderURL.indexOf("ssl://") >= 0) {
           SSLParams ssl = new SSLParams(args);

           ssl.init();
        }
        */
    }

}



