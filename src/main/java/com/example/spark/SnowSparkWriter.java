package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class SnowSparkWriter {
    private final Properties properties;

    public SnowSparkWriter(Properties properties) {
        this.properties = properties;
    }

    public void write(Dataset<Row> dataset, String jdbcUrl, String table) {
        dataset.write().mode(SaveMode.Append).jdbc(jdbcUrl, table, properties);
    }
}
