package com.tibco.mcqueary.jmsperf;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.naming.Context;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.PropertiesConfigurationLayout;

import com.tibco.mcqueary.jmsperf.Constants.Provider;

public class PropertyFileGenerator {

	private final static String OUTPUT_DIRECTORY = "src/test/resources/";
//	private final static String TEMPLATE_FILENAME = OUTPUT_DIRECTORY+"template.properties";
	private final static String MANYHASHES = "################################################################################";
	private final static String NL = "\n";
	public static void listConfig(PropertiesConfiguration p)
	{
		System.out.println("###########");
		for (Iterator<String> keys=p.getKeys(); keys.hasNext();)
		{
			String key = keys.next();
			System.out.println(key + "=" + p.getString(key));
		}		
		System.out.println();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertiesConfiguration templateConfig;
		ConfigHandler handler = ConfigHandler.instance(ConfigHandler.KEY_ALL);
		templateConfig=handler.getInstanceConfig();

		try {

			PropertiesConfigurationLayout defaultLayout = new PropertiesConfigurationLayout(templateConfig);
			String headerComment = "#\n" + Executive.getVersionInfo()+"\n#"
					+ "\n# Sample properties file\n#";
			defaultLayout.setHeaderComment(headerComment);
			
			List<String> propertyNames = handler.getPropertyKeys(ConfigHandler.KEY_ALL);
			for (String name : propertyNames)
			{
//				Configuration p=handler.getOptionConfiguration(name);
				String description=handler.getPropertyDescription(name);
				if (description==null)
					continue;
				List<Object> validVals = Arrays.asList(handler.getValidValues(name));
				String comment = MANYHASHES+NL+description;
				if ((validVals != null) &&(validVals.size() > 0))
				{
					comment += NL+"Valid values include:";
					for (Iterator<Object> it = validVals.iterator(); it.hasNext();)
					{
						String val = (String)it.next();
						comment += " " + val;
						if (it.hasNext())
							comment +=",";
					}
				}
				comment += NL+MANYHASHES+NL;
				defaultLayout.setComment(name, comment);
				defaultLayout.setBlancLinesBefore(name, 1);
			}
//			PrintWriter writer = new PrintWriter(TEMPLATE_FILENAME, "UTF-8");
System.err.println("==== beginning of defaultLayout ====");	
			PrintWriter outWriter = new PrintWriter(System.err);
			defaultLayout.save(outWriter);
System.err.println("==== end of defaultLayout ====");			
			PropertiesConfiguration tibcoConfig = new PropertiesConfiguration();
			tibcoConfig.copy(templateConfig);
			tibcoConfig.setProperty(Constants.PROP_PROVIDER_NAME, Provider.TIBEMS);
			tibcoConfig.setProperty(Constants.PROP_PROVIDER_URL, Provider.TIBEMS.url());
			tibcoConfig.setProperty(Constants.PROP_PROVIDER_CONTEXT_FACTORY, 
					Provider.TIBEMS.factory());
			PropertiesConfigurationLayout tibcoLayout = 
					new PropertiesConfigurationLayout(tibcoConfig,defaultLayout);
			tibcoConfig.setLayout(tibcoLayout);
			tibcoConfig.setFileName(OUTPUT_DIRECTORY+"tibems.properties");
			tibcoConfig.save();
		
			System.out.println("\n\n\nCONFIGURATION FOR TIBCO:");
			tibcoConfig.save(System.out);
			
			PropertiesConfiguration kaazingConfig = new PropertiesConfiguration();
			kaazingConfig.copy(templateConfig);
			kaazingConfig.setProperty(Constants.PROP_PROVIDER_NAME, Provider.KAAZING);
			kaazingConfig.setProperty(Constants.PROP_PROVIDER_URL, Provider.KAAZING.url());
			kaazingConfig.setProperty(Constants.PROP_PROVIDER_CONTEXT_FACTORY, 
					Provider.KAAZING.factory());
			kaazingConfig.setProperty(Constants.PROP_PROVIDER_TOPIC_FORMAT, "/topic/%s");
			kaazingConfig.setProperty(Constants.PROP_PROVIDER_QUEUE_FORMAT, "/queue/%s");
			PropertiesConfigurationLayout kaazingLayout = 
					new PropertiesConfigurationLayout(kaazingConfig,defaultLayout);
			kaazingConfig.setLayout(kaazingLayout);
			kaazingConfig.setFileName(OUTPUT_DIRECTORY+"kaazing.properties");
			kaazingConfig.save();
			
			System.out.println("\n\n\nCONFIGURATION FOR KAAZING:");
			kaazingConfig.save(System.out);
						
			PropertiesConfiguration activemqConfig = new PropertiesConfiguration();
			activemqConfig.copy(templateConfig);
			activemqConfig.setProperty(Constants.PROP_PROVIDER_NAME, Provider.ACTIVEMQ);
			activemqConfig.setProperty(Constants.PROP_PROVIDER_URL, Provider.ACTIVEMQ.url());
			activemqConfig.setProperty(Constants.PROP_PROVIDER_CONTEXT_FACTORY, 
					Provider.ACTIVEMQ.factory());
			PropertiesConfigurationLayout activemqLayout = 
					new PropertiesConfigurationLayout(activemqConfig,defaultLayout);
			activemqConfig.setLayout(activemqLayout);
			activemqConfig.setFileName(OUTPUT_DIRECTORY+"activemq.properties");
			activemqConfig.save();
			
			System.out.println("\n\n\nCONFIGURATION FOR ACTIVEMQ:");
			activemqConfig.save(System.out);
			
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
	}

}
