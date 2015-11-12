package db.pool;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class PoolProperties {
	
	private static Properties properties;
	
	public PoolProperties(Reader resource) throws IOException{
		properties = new Properties();
		properties.load(resource);
	}
	
	public static String getProperty(String propertName){
		return properties.getProperty(propertName);
	}
	
	public static int getIntProperty(String propertName, int defaultValue){
		try{
			return Integer.parseInt(properties.getProperty(propertName));
		}catch(NumberFormatException ne){
			return defaultValue;
		}
	}

}
