# Knapsack 0-1 function wieights, values and size n.
import sys
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


# Greedy implementation of 0-1 Knapsack algorithm.
def knapsack(knapsackDF, W):
    ratioDF = knapsackDF.withColumn("ratio", lit(knapsackDF.values / knapsackDF.weights)).sort(col("ratio").desc())

    ratioDF.registerTempTable("tempTable")
    df2 = sqlContext.sql("""
        SELECT
            item,
            weights,
            values,
            ratio,
            sum(ratio) OVER (PARTITION BY item ORDER BY ratio) as cumsum
        FROM
        tempTable
        """)

    # partialSumsDF = (ratioDF
    #     .map(lambda x: x)
    #  )
    return df2


# Setup sample data for knapsack.
knapsackData = [('thing1', 1, 2), ('thing2', 2, 3), ('thing3', 4, 5)]
# Make a dataframe with item(s), weight(s), and value(s) for the knapsack.
knapsackData = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])

# Call the knapsack greedy function, with data and size 5.
k = knapsack(knapsackData, 5.0)
print k.take(3)


