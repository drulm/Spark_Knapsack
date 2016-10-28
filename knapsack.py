
# Knapsack 0-1 function wieights, values and size n.
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

# Greedy implementation of 0-1 Knapsack algorithm.
def knapsack(knapsackDF, W):
    ratioDF = knapsackDF.withColumn("ratio", lit(knapsackDF.values / knapsackDF.weights))
    ratioDF.sort(col("ratio").desc())
    partialSumsDF = (ratioDF
         .map(lambda x: x)
      )
    return partialSumsDF


knapsackData = [('thing1', 1, 2), ('thing2', 2, 3), ('thing3', 4, 5)]
knapsackData = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])
k = knapsack(knapsackDF, 5)
print k.take(3)


