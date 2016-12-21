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
println("\n")

// Create a random maximum weight
val start = N * 0.3
val end = N * 0.6
val W = (math.random * (end - start) + start)

// Show the weight.
println("W: ")
println(W)
println("\n")

// Call the knapsack greedy approximation function, with data and size 5.
val knapTotals = List()
val knapResults = knapsack.knapsackApprox(knapsackData, W, knapTotals)

// Show the results Dataframe.
println("Selected Elements:")
knapResults.show()
println("\n")

// Show totals for selected elements of knapsack.
println("Totals:")
println(knapTotals)
println("\n")

// ------------------------------------------
// End of Approximate Knapsack function test
// ------------------------------------------

