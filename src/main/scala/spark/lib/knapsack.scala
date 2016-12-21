/*
Copyright 2016 Darrell Ulm

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
  limitations under the License.
*/
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

/*
  A simple greedy parallel implementation of 0-1 Knapsack algorithm.

  Parameters
  ----------
  spark: SparkSession
    A Spark-Session

  knapsackDF : Spark Dataframe with knapsack data
    sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])

  W : float
    Total weight allowed for knapsack.

  knapTotals : list
    List of result totals of knapsack values and weights.

  Returns : Dataframe
  -------
    Dataframe with results.
*/
object knapsack {

  def knapsackApprox(knapsackDF: DataFrame, W: Double): DataFrame = {

    // Add ratio of values / weights column.
    val ratioDF = knapsackDF.withColumn("ratio", knapsackDF("values") / knapsackDF("weights"))
    val newRatioDF = (ratioDF
      .filter(ratioDF("weights") <= W)
      .sort(ratioDF("ratio").desc)
      )

    // An sql method to calculate the partial sums of the ratios.
    newRatioDF.createOrReplaceTempView("tempTable")

    val partialSumWeightsDF = spark.sql("SELECT item, weights, values, ratio, sum(weights) OVER (ORDER BY ratio desc) as partSumWeights FROM tempTable")

    // Get the max number of items, less than or equal to W in Spark.
    val partialSumWeightsFilteredDF = (
      partialSumWeightsDF
        // partialSumWeightsDF.sort(partialSumWeightsDF("ratio").desc)
        .filter(partialSumWeightsDF("partSumWeights") <= W)
      )

    // Return the solution elements with total values, weights and count.
    partialSumWeightsDF
  }

}

