package com.tibco.mcqueary.jmsperf;

import org.kohsuke.args4j.*;

public class Tester {

	   @Option(name="-h", usage="host of the external system", required=true)
	   private String host;

	   @Option(name="-p", usage="port of the external system")
	   private int port = 80;

	   public void setHost(String h){ this.host = h; }
	   public void setPort(int p){ this.port = p; }

	   public static void main(String[] args) {

	       Tester t = new Tester();
	       t.run(args);
	   }


	   public void run(String[] args) {

	       CmdLineParser parser = new CmdLineParser(this);
	       try {
	           parser.parseArgument(args);

	           //do whatever ...
	           System.out.println("Testing host " + host + " on port " + port);

	       } catch (CmdLineException e) {
	         
	           System.err.println(e.getMessage());
	           System.err.println("\nTester [options...] arguments...");
	           parser.printUsage(System.err);
	       }
	   }
	}