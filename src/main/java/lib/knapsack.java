package com.github.drulm.Spark_Knapsack;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class knapsack {
    public static DataFrame knapsackApprox(DataFrame knapsackDF, double W) {

        SparkConf conf = new SparkConf().setAppName("Knapsack Approx").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }


}
