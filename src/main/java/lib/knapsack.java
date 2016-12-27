package com.github.drulm.Spark_Knapsack

import org.apache.spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class knapsack {
    public static DataFrame knapsackApprox(DataFrame knapsackDF, double W) {

        SparkConf conf = new SparkConf().setAppName("Knapsack Approx").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }


}
