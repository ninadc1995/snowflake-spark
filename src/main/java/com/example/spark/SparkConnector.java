package com.example.spark;

import com.example.constants.Constants;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkConnector {
    private static final Logger logger = LoggerFactory.getLogger(SparkConnector.class);
    public static boolean underTest = false;

    public static SparkSession getSparkSession() {
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
}
