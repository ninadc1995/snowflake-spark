package com.example.utils;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private Util() {
    }

    public static void initLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("net.snowflake.client").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(Level.OFF);
    }


    public static void loadProperties(String config, Properties properties) {
        try {
            logger.info("reading config file {} ", config);
            FileInputStream input = new FileInputStream(config);
            properties.load(input);
        } catch (IOException e) {
            logger.error("Unable to load properties..");
            e.printStackTrace();
            System.exit(1);
        }
    }


}
