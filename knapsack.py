#PySpark Test Functions for Knapsack in Progress


# Knapsack 0-1 function wieights, values and size n.
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

# Greedy implementation of 0-1 Knapsack algorithm.
def knapsack(knapsackDF, W):
    knapsackDF.withColumn("ratio", lit(knapsackDF.values / knapsackDF.weights))
    knapsackDF.sort(col("ratio").desc())
    k = 0
    return k


knapsackData = [('thing1', 1, 2), ('thing2', 2, 3), ('thing3', 4, 5)]
knapsackDF = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])
print weightsDF.count()
k = knapsack(knapsackDF, 5)
print k

