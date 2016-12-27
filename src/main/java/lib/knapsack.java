package com.github.drulm.Spark_Knapsack

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class knapsackApprox {
    SparkConf conf = new SparkConf().setAppName("Knapsack Approx").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);


}
