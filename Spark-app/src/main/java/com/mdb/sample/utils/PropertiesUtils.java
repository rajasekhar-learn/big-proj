package com.mdb.sample.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * load properties from class path and make them available.
 */
public class PropertiesUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtils.class);
    private final Properties properties;

    /**
     * inner static class for singleton reference.
     */
    static class PropertiesUtilsReferenceHolder {
        public static PropertiesUtils propertiesUtils = new PropertiesUtils();
    }

    private PropertiesUtils() {
        this.properties = loadProperties();
    }

    /**
     * loading application.properties from class path of Driver or Executor.
     *
     * @return
     */
    private static Properties loadProperties() {
        Properties appProps = new Properties();
        try {
            appProps.load(PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            LOGGER.error("couldn't load application properties !! {}", e.getMessage(), e);
        }
        return appProps;
    }

    public static String getPropertyValue(String key) {
        return PropertiesUtilsReferenceHolder.propertiesUtils.properties.getProperty(key);
    }

    /**
     * getPropertyKeyAndValuesByPrefix method helps to retrieve all properties start with prefix,
     * provide option to exclude prefix from keys. this helps to retrieve different set of same properties for different purposes.
     *
     * @param prefix
     * @param excludePrefixInResultKey
     * @return
     */
    public static Map<String, String> getPropertyKeyAndValuesByPrefix(String prefix, boolean excludePrefixInResultKey) {
        return PropertiesUtilsReferenceHolder.propertiesUtils.properties.entrySet().stream()
                .filter(entry -> String.valueOf(entry.getKey()).startsWith(prefix))
                .collect(Collectors.toMap(
                        entry -> excludePrefixInResultKey ?
                                String.valueOf(entry.getKey()).substring(prefix.length() + 1)
                                : String.valueOf(entry.getKey()),
                        entry -> String.valueOf(entry.getValue())));
    }

}
