package kafka.common;

import org.apache.commons.configuration.ConfigurationException;

public abstract class BaseApplication {

    protected BaseApplication() throws ConfigurationException {
        PropertyUtils.loadProperties();
    }

    public abstract void execute();
}
