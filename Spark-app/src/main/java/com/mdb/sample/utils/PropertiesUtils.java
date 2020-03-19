package com.mdb.sample.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * load properties from class path and make them available.
 */
public class PropertiesUtils {

    private static final Logger LOGGER= LoggerFactory.getLogger(PropertiesUtils.class);
    private final Properties properties;
    /**
     * inner static class for singleton reference.
     */
    static class PropertiesUtilsReferenceHolder{
        public static PropertiesUtils propertiesUtils=new PropertiesUtils();
    }
    private static final PropertiesUtils INSTANCE=PropertiesUtilsReferenceHolder.propertiesUtils;
    private PropertiesUtils(){
        this.properties=loadProperties();
    }
    /**
     * loading application.properties from class path of Driver or Executor.
     * @return
     */
    private static Properties loadProperties() {
        Properties appProps = new Properties();
        try {
            appProps.load(PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties"));
        }catch (IOException e){
            LOGGER.error("couldn't load application properties !! {}",e.getMessage(),e);
        }
        return appProps;
    }

    public static String getPropertyValue(String key){
       return INSTANCE.properties.getProperty(key);
    }

}
