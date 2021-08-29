package com.example;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private Util() {
    }

    public static SparkSession getSparkSession(boolean underTest) {
        if (underTest) {
            logger.info("creating local spark session...");
            return SparkSession.builder()
                    .master(Constants.LOCAL)
                    .appName(Constants.APP_NAME)
                    .getOrCreate();
        } else {
            logger.info("creating spark session: master -> yarn and hive support...");
            return SparkSession.builder()
                    .master(Constants.YARN)
                    .appName(Constants.APP_NAME)
                    .enableHiveSupport()
                    .getOrCreate();
        }
    }

    public static void loadProperties(String config, Properties properties) throws IOException {
        logger.info("reading config file {} ", config);
        FileInputStream input = new FileInputStream(config);
        properties.load(input);
    }

}
