# Knapsack 0-1 function wieights, values and size n.
import sys
import random
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


# Greedy implementation of 0-1 Knapsack algorithm.
def knapsack(knapsackDF, W, N):
    # Add ratio of values / weights column.
    ratioDF = (knapsackDF.withColumn("ratio", lit(knapsackDF.values / knapsackDF.weights))
               .filter(col("weights") <= W)
               .sort(col("ratio").desc())
               )

    print "ratio of values / weights column"
    print ratioDF.take(N)
    print "\n"

    # Calculate the partial sums of the ratios.
    ratioDF.registerTempTable("tempTable")
    partialSumWeightsDF = sqlContext.sql("""
        SELECT
            item,
            weights,
            values,
            ratio,
            sum(weights) OVER (ORDER BY ratio desc) as partSumWeights
        FROM
        tempTable
        """)

    print "Calculate the partial sums of the ratios."
    print partialSumWeightsDF.take(N)
    print "\n"

    # Get the max number of items, greedy less than or equal to W in Spark.
    partialSumWeightsFilteredDF = partialSumWeightsDF.sort(col("ratio").desc()).filter(col("partSumWeights") <= W)

    # Return the elemtns
    return partialSumWeightsFilteredDF


# Test the Knapsack function

# Problem size
N = 5

# Setup sample data for knapsack.
# knapsackData = [('thing1', 1, 2), ('thing2', 2, 3), ('thing3', 4, 5), ('thing4', 4, 5), ('thing5', 4, 5), ('thing6', 4, 5)]
knapsackData = [('item_' + str(k), random.randint(1, 10), random.randint(1, 10)) for k in range(N)]

# Make a dataframe with item(s), weight(s), and value(s) for the knapsack.
knapsackData = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])
print "Original Data"
print knapsackData.take(N)
print "\n"

W = random.randint(N * 2, N * 5)
print "W = ", W
print "\n"

# Call the knapsack greedy function, with data and size 5.
k = knapsack(knapsackData, W, N)
print "Selected Elements"
print k.take(N)

# @TODO
# Calculate the sum of values.


