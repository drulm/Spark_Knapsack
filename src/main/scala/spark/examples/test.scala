// -----------------------------------------------------------------
//import spark.lib.knapsack
//import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.random.RandomRDDs._
import scala.collection.mutable.ListBuffer

/*
  --------------------------------------------
  Test the Approximate Knapsack function test
  --------------------------------------------
*/

// Pull in the knapsack library.
// import spark.lib.knapsack

// Knapsack problem size.
val N = 10

// Random
val r = scala.util.Random

// Setup sample data for knapsack.
val knapsackDataListBuffer = ListBuffer[(String, Double, Double)]()
for (k <- 1 to N) {
  knapsackDataListBuffer += (("item_" + k.toString, r.nextDouble() * 10.0, r.nextDouble() * 10.0))
}
val knapsackDataList = knapsackDataListBuffer.toList

// Make a Dataframe with item(s), weight(s), and value(s) for the knapsack.
val knapsackData = sc.parallelize(knapsackDataList).toDF("item", "weights", "values")

// Display the original data
println("Original Data:")
knapsackData.show()
println("\r\n")

// Create a random maximum weight
val start = N * 0.3
val end = N * 0.6
val W = (math.random * (end - start) + start)

// Show the weight.
println("W: ")
println(W)
println("\r\n")

// Call the knapsack greedy approximation function, with data and size 5.
val knapResults = knapsack.knapsackApprox(knapsackData, W)

// Show the results Dataframe.
println("Selected Elements:")
knapResults.show()
println("\r\n")

// Find the totals.
val valuesResult = knapResults.agg(sum("values"))
val weightsResult = knapResults.agg(sum("weights"))
val countResult = knapResults.count()

// Show totals for selected elements of knapsack.
println("Value Total:")
valuesResult.show()
println("\r\n")
println("Weights Total:")
weightsResult.show()
println("\r\n")
println("Count:")
println(countResult)
println("\r\n")

// ------------------------------------------
// End of Approximate Knapsack function test
// ------------------------------------------

