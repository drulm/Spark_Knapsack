# --------------------------------------------
# Test the Approximate Knapsack function test
# --------------------------------------------

# Pull in the knapsack library.

import random
from pyspark.sql import SparkSession

from knapsack import knapsack

# Create the SparkContext.
sc = SparkSession \
    .builder \
    .appName("Knapsack Approximation Algorithm Test") \
    .getOrCreate()

# Knapsack problem size.
N = 10

# Setup sample data for knapsack.
knapsackData = [('item_' + str(k), random.uniform(1.0, 10.0), random.uniform(1.0, 10.0)) for k in range(N)]

# Make a Dataframe with item(s), weight(s), and value(s) for the knapsack.
knapsackData = sc.createDataFrame(knapsackData, ['item', 'weights', 'values'])

# Display the original data
print "Original Data:"
print knapsackData.show()
print "\n"

# Create a random maximum weight
W = random.uniform(N * 1.3, N * 1.6)

# Show the weight.
print "W: "
print W
print "\n"

# Call the knapsack greedy approximation function, with data and size 5.
k = knapsack.knapsackApprox(knapsackData, W)

# Show the results Dataframe.
print "Selected Elements:"
print k.show()
print "\n"

# Show totals for selected elements of knapsack.
sumValues = k.rdd.map(lambda x: x["values"]).reduce(lambda x, y: x+y)
sumWeights = k.rdd.map(lambda x: x["weights"]).reduce(lambda x, y: x+y)
numResults = k.count()
print "Totals:"
print "Sum Values: ",  sumValues
print "Sum Weights: ",  sumWeights
print numResults
print "\n"

# ------------------------------------------
# End of Approximate Knapsack function test
# ------------------------------------------
