package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SnowSparkReader {

    private final SparkSession spark;
    private final Properties properties;

    public SnowSparkReader(SparkSession spark, Properties properties) {
        this.spark = spark;
        this.properties = properties;
    }

    public Dataset<Row> read(String jdbcUrl, String tableName) {
        return spark.read().jdbc(jdbcUrl, tableName, properties);

    }

}
