package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class SnowSparkReader {

    private final Properties properties;

    public SnowSparkReader(Properties properties) {
        this.properties = properties;
    }

    public Dataset<Row> read(String jdbcUrl, String tableName) {
        return SparkConnector.getSparkSession().read().jdbc(jdbcUrl, tableName, properties);
    }

}
