package com.example;

import com.example.constants.Constants;
import com.example.spark.SnowSparkReader;
import com.example.spark.SnowSparkWriter;
import com.example.spark.SparkConnector;
import com.example.utils.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Main {
    private static final Properties properties = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(SnowSparkReader.class);


    public static void main(String[] args) {
        SparkConnector.underTest = Boolean.parseBoolean(args[0]);
        String config = args[1];

        Util.initLogger();
        Util.loadProperties(config, properties);

        logger.info("reading data from snowflake table...");
        SnowSparkReader reader = new SnowSparkReader(properties);
        Dataset<Row> ds = reader.read(properties.getProperty(Constants.DB_URL),
                properties.getProperty(Constants.INPUT_TABLE));

        ds.show(false);

        logger.info("writing data to snowflake table...");
        SnowSparkWriter writer = new SnowSparkWriter(properties);
        writer.write(ds, properties.getProperty(Constants.DB_URL),
                properties.getProperty(Constants.OUTPUT_TABLE));

    }

}
