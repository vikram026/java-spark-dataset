package com.example;

import com.example.service.SparkService;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaSparkApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaSparkApp.class);

    public static void main(String[] args) {
        LOGGER.info("Hello Java Spark Integration Application!");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaSparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf)
                .config("spark.sql.warehouse.dir", "/temp/java_sparkgradleapp")
                .getOrCreate();
        LOGGER.info(sparkSession.logName());
        new SparkService().sparkJob(sparkSession);
    }
}
