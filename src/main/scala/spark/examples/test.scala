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
val knapsackDataList = new ListBuffer[(String, Float, Float)]()
for(k <- 1 to 10) {
  val knapsackDataList += ("item_" ++ str(k), r.nextFloat(10), r.nextFloat(10))
}
val knapsackData = knapsackDataList.toList

// Make a Dataframe with item(s), weight(s), and value(s) for the knapsack.
val knapsackData = sc.parallelize(knapsackDataList).toDF("item", "weights", "values")

// Display the original data
println("Original Data:")
println(knapsackData.take(N))
println("\n")

// Create a random maximum weight
val start = N * 0.3
val end = N * 0.6
val W = (math.random * (end - start) + start);

// Show the weight.
println("W: ")
println(W)
println("\n")

// Call the knapsack greedy approximation function, with data and size 5.
val knapTotals = Array()
//val knapResults = knapsack.knapsackApprox(knapsackData, W, knapTotals)

// Show the results Dataframe.
// println("Selected Elements:")
// println(knapResults.take(N))
// println("\n")

// Show totals for selected elements of knapsack.
println("Totals:")
println(knapTotals)
println("\n")

// ------------------------------------------
// End of Approximate Knapsack function test
// ------------------------------------------

