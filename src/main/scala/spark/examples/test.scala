/*
  --------------------------------------------
  Test the Approximate Knapsack function test
  --------------------------------------------
*/

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.random.RandomRDDs._

// Pull in the knapsack library.
import spark.lib.knapsack

// Create the SparkContext.
val sc = SparkSession \
  .builder \
  .appName("Knapsack Approximation Algorithm Test") \
  .getOrCreate()

// Knapsack problem size.
val N = 10

// Setup sample data for knapsack.
val knapsackData = [('item_' + str(k), random.uniform(1.0, 10.0), random.uniform(1.0, 10.0)) for k in range(N)]

// Make a Dataframe with item(s), weight(s), and value(s) for the knapsack.
val knapsackData = sc.createDataFrame(knapsackData, ['item', 'weights', 'values'])

// Display the original data
println("Original Data:")
println(knapsackData.take(N))
println("\n")

// Create a random maximum weight
val W = random.uniform(N * 0.3, N * 0.6)

// Show the weight.
println("W: ")
println(W)
println("\n")

// Call the knapsack greedy approximation function, with data and size 5.
val knapTotals = []
val k = knapsack.knapsackApprox(knapsackData, W, knapTotals)

// Show the results Dataframe.
println("Selected Elements:")
println(k.take(N))
println("\n")

// Show totals for selected elements of knapsack.
println("Totals:")
println(knapTotals)
println("\n")

# ------------------------------------------
# End of Approximate Knapsack function test
# ------------------------------------------

