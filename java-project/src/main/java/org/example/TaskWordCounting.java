package org.example;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class TaskWordCounting {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskWordCounting.class);

    static class WordMapper implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            // Remove punctuations from the string
            s.replaceAll("[^a-zA-Z0-9\\s]","");

            // Convert to lower case
            s.toLowerCase();

            // Split it by whitespace
            List<String> intermediaryResult = new ArrayList<>();
            intermediaryResult = Arrays.asList(s.split("\\s+"));

            // Put all strings that are some sequence of alphanumeric characters to list
            List<String> results = new ArrayList<>();
            for (String word : intermediaryResult) {
                if (word.matches("[a-zA-Z0-9]+")) {
                    results.add(word);
                }
            }
            return results.iterator(); // return the list as an iterator
        }

    }

    public static void run(boolean local) throws IOException {

        LOGGER.info("Starting the task");

        SparkConf sparkConf = null;

        String datasetFileName = "dataset-wordcount.txt";
        String datasetFilePath ="datasets/" + datasetFileName;
        String applicationName = "WordCount";
        String hdfsDatasetPath = "hdfs://namenode:9000/datasets/";
        String sparkMaster = "spark://spark-master:7077";

        Date t0 = new Date(); //Mark the start timestamp

        if(local){
            sparkConf = new SparkConf().setAppName(applicationName).setMaster("local[*]");
        }else {
            datasetFilePath = hdfsDatasetPath + datasetFileName;
            sparkConf = new SparkConf().setAppName(applicationName).setMaster(sparkMaster);
        }

        JavaSparkContext sparkContext =  new JavaSparkContext(sparkConf);
        //sparkContext.setLogLevel("WARN");
        LOGGER.info("Loading text file");
        JavaRDD<String> textFile = sparkContext.textFile(datasetFilePath, 3);

        //=================================== Your code now =========================================

        //Step-A: using the available textFile, create a flat map of words by calling the WordMapper.
        LOGGER.info("Flat mapping to create word list");
        JavaRDD<String> words = textFile.flatMap(new WordMapper());

        //-------------------------------------------------------------------------------------------
        Date t1 = new Date();
        LOGGER.info("Word list created in {}ms", t1.getTime()-t0.getTime());


        //Step B: Now invoke a mapping function that will create key value-pair for each word in the list
        LOGGER.info("Mapping function");
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        //-------------------------------------------------------------------------------------------
        Date t2 = new Date();
        LOGGER.info("Mapping invoked in {}ms", t2.getTime()-t1.getTime());

        //Step C: Invoke a Reduce function that will sum up the values (against each key)
        LOGGER.info("Reducing function");
        JavaPairRDD<String, Integer> summedValues = wordPairs.reduceByKey((a,b) -> a + b);

        //-------------------------------------------------------------------------------------------
        Date t3 = new Date();
        LOGGER.info("Reduce invoked in {}ms", t3.getTime()-t2.getTime());

        //Step D: Finally, output the counts for each word
        LOGGER.info("Collecting to driver");
        List<Tuple2<String, Integer>> output = summedValues.collect();
        for (Tuple2<String, Integer> tuple : output) {
            System.out.println(tuple._1() + " : " + tuple._2());
        }

        //-------------------------------------------------------------------------------------------
        Date t4 = new Date();
        LOGGER.info("Application completed in {}ms", t4.getTime()-t0.getTime());

        //If you want you can save the counts to a hdfs file
        if(!local) {
            //counts.repartition(1).saveAsTextFile("hdfs://namenode:9000/output/counts.txt");
        }
        sparkContext.stop();
        sparkContext.close();
    }
}