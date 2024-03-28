# Using Spark for Joins - Map Side and Reduce Side
from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Joins")

# Create RDDs for left and right datasets
left_data = sc.parallelize([(1, "A"), (2, "B"), (3, "C")])
right_data = sc.parallelize([(1, "X"), (2, "Y"), (4, "Z")])

# Perform map-side join
map_join = left_data.join(right_data)

# Perform reduce-side join
reduce_join = left_data.union(right_data).reduceByKey(lambda x, y: (x, y))

# Print the results
print("Map Side Join:", map_join.collect())
print("Reduce Side Join:", reduce_join.collect())

# Stop SparkContext
sc.stop()
