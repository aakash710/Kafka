package kafka.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class PropertyUtils {

    private static final String propertyFilePath = "property.config";

    private static PropertiesConfiguration configuration;

    static void loadProperties() throws ConfigurationException {
        configuration = new PropertiesConfiguration(System.getProperty(propertyFilePath));
    }

    public static String getProperty(String propertyName) {
        return configuration.getProperty(propertyName).toString();
    }
}
