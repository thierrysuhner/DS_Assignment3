package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*; // Import Spark SQL functions

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TaskExampleShowering {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExampleShowering.class);
    public static void run(boolean local){
        Date startTime = new Date();
        SparkSession sparkSession = null;

        try {
            LOGGER.info("Starting Spark Application.");

            String sparkApplicationName = "ShowerTelemetry";
            String datasetFileName = "dataset-showering.csv";
            String datasetFilePath ="../datasets/" + datasetFileName;
            String hdfsDatasetPath = "hdfs://namenode:9000/datasets/";
            String sparkMasterUrl = "spark://spark-master:7077";

            SparkConf sparkConf = null;

            if(local){
                sparkConf = new SparkConf().setAppName(sparkApplicationName).setMaster("local[*]");
            }else {
                sparkConf = new SparkConf().setAppName(sparkApplicationName).setMaster(sparkMasterUrl);
            }

            // Initialize SparkSession using the configuration
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            LOGGER.info("SparkSession initialized.");

            Dataset<Row> df = DatasetHelper.getDataset(sparkSession, datasetFileName, local);

            // --- Determine Yearly Pattern ---
            // Extract month from timestamp and calculate average temperature per month
            LOGGER.info("\n--- Analyzing Yearly Temperature Patterns (Average Temperature by Month) ---");
            Dataset<Row> yearlyPattern = df
                    .groupBy("month")
                    .agg(avg("temperature").as("avg_temp_celsius"))
                    .orderBy("month")
                    .filter(col("month").gt(0));

            //yearlyPattern.explain();

            LOGGER.info("\nYearly Temperature Pattern Results:");
            yearlyPattern.show();
            Date endTime = new Date();

            System.out.printf("Completion Time: %d\n", endTime.getTime() - startTime.getTime());
            LOGGER.info("\nCongratulations! You have run your first Spark application in {} mode", local ? "local" : "cluster");

        } catch (Exception e) {
            LOGGER.error("An error occurred during Spark application execution: " + e.getMessage(), e);
        } finally {
            if (sparkSession != null) {
                sparkSession.stop();
                LOGGER.info("SparkSession stopped.");
            }
        }
    }
}
