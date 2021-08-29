package com.example;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


public class Main {
    private static final Properties properties = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(SnowSparkReader.class);

    private static void initLogger() {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("net.snowflake.client").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(Level.OFF);
    }

    private static void initProperties(String config) {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            Util.loadProperties(config, properties);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Unable to load properties..");
            e.printStackTrace();
            System.exit(1);
        }
    }


    public static void main(String[] args) {
        boolean underTest = Boolean.parseBoolean(args[0]);
        String config = args[1];

        initLogger();
        initProperties(config);

        SparkSession spark = Util.getSparkSession(underTest);


        SnowSparkReader reader = new SnowSparkReader(spark, properties);
        Dataset<Row> ds = reader.read(properties.getProperty(Constants.DB_URL),
                properties.getProperty(Constants.INPUT_TABLE));
        ds.show(false);
        SnowSparkWriter writer = new SnowSparkWriter(properties);
        writer.write(ds, properties.getProperty(Constants.DB_URL),
                properties.getProperty(Constants.OUTPUT_TABLE));

    }

}
