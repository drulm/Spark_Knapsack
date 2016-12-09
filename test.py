# ----------------------------
# Test the Knapsack function
# ----------------------------
# @TODO Split out function code, and test code.

# Pull in the knapsack library.
import knapsack

import random
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Get the SparkContext and sqlContext.
sc =SparkContext()
sqlContext = SQLContext(sc)

# Problem size
N = 10

# Setup sample data for knapsack.
knapsackData = [('item_' + str(k), random.uniform(1.0, 10.0), random.uniform(1.0, 10.0)) for k in range(N)]

# Make a dataframe with item(s), weight(s), and value(s) for the knapsack.
knapsackData = sqlContext.createDataFrame(knapsackData, ['item', 'weights', 'values'])

# Display the original data
print "Original Data:"
print knapsackData.take(N)
print "\n"

# Create a random weight
W = random.uniform(N * 0.3, N * 0.6)

# Show the weight
print "W: "
print W
print "\n"

# Call the knapsack greedy function, with data and size 5.
knapTotals = []
k = knapsack(knapsackData, W, knapTotals)

# Show the results datafram
print "Selected Elements:"
print k.take(N)
print "\n"

# Show totals for selected elements of knapsack
print "Totals:"
print knapTotals
print "\n"

