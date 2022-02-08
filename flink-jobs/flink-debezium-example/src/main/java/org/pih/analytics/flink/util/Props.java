package org.pih.analytics.flink.util;

import org.pih.analytics.flink.DebeziumFlinkConnector;

import java.util.Properties;

public class Props {

    public static Properties readResource(String resourceName) {
        try {
            Properties props = new Properties();
            props.load(DebeziumFlinkConnector.class.getResourceAsStream(resourceName));
            return props;
        }
        catch(Exception e) {
            throw new RuntimeException("Unable to load Properties from " + resourceName, e);
        }
    }
}
