package com.nikhilsujith;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public  class KafkaConfigUtil {
    private static Properties readConfig(final String configFile) throws IOException {
        // reads the client configuration from client.properties
        // and returns it as a Properties object
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found");
        }

        final Properties config = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            config.load(inputStream);
        }

        return config;
    }

    public static Properties getKafkaClientProps(String configFile) throws IOException {
        try {
            String topic = "";
            final Properties config = readConfig(configFile);
            return config;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IOException("Unable to read client.properties");
    }
}
