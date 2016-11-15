# Knapsack 0-1 function wieights, values and size n.
import sys
import random
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


# Greedy implementation of 0-1 Knapsack algorithm.
def knapsack(knapsackDF, W):
    # Add ratio of values / weights column.
    ratioDF = knapsackDF.withColumn("ratio", lit(knapsackDF.values / knapsackDF.weights)).sort(col("ratio").desc())

    # Calculate the partial sums of the ratios.
    ratioDF.registerTempTable("tempTable")
    RatioPartialSumDF = sqlContext.sql("""
        SELECT
            item,
            weights,
            values,
            ratio,
            sum(ratio) OVER (ORDER BY ratio) as partSum
        FROM
        tempTable
        """)

    # Get the max number of items, greedy less than or equal to W in Spark.
    RatioPartialSumDF = RatioPartialSumDF.filter(col("partSum") <= W)
    return RatioPartialSumDF


# Test the Knapsack function

# Problem size
N = 10

# Setup sample data for knapsack.
# knapsackData = [('thing1', 1, 2), ('thing2', 2, 3), ('thing3', 4, 5), ('thing4', 4, 5), ('thing5', 4, 5), ('thing6', 4, 5)]
knapsackData = [('item_' + str(k), random.randint(1, 100), random.randint(1, 100)) for k in range(N)]

# Make a dataframe with item(s), weight(s), and value(s) for the knapsack.
knapsackData = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])

# Call the knapsack greedy function, with data and size 5.
k = knapsack(knapsackData, random.randint(1, 100 * N / 2))
print k.take(N)


