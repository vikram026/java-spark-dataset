package com.example.service;

import com.example.model.Employee;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class SparkService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkService.class);

    public void sparkJob(final SparkSession sparkSession) {
        LOGGER.info("Spark Job Started....!");
        try {
            SQLContext sqlContext = sparkSession.sqlContext();
            LOGGER.info("Preparing Datasets of String Type");
            List<String> list = Arrays.asList("ABC", "ASD", "WER");
            Dataset<String> dataFrame = sparkSession.createDataset(list, Encoders.STRING());
            dataFrame.show();
            LOGGER.info("Preparing Datasets of Employee Type");
            List<Employee> employees = Arrays.asList(new Employee(101, "ABC", "DS1"), new Employee(102, "ASE", "DS2"), new Employee(103, "WSD", "DS3"));
            Dataset<Employee> dataset = sparkSession.createDataset(employees, Encoders.bean(Employee.class));
            dataset.show();
            LOGGER.info("Preparing SparkSQL to get dataset based on SQL query");
            dataset.createOrReplaceTempView("employee");
            Dataset<Employee> employeeDataset = sqlContext.sql("select * from employee where employeeId = 102").as(Encoders.bean(Employee.class));
            employeeDataset.show();
            sqlContext.clearCache();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
            sparkSession.close();
            LOGGER.info("Stop Spark Job");
        }
    }
}
