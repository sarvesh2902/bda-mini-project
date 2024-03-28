from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("MatrixVectorMultiplication")
sc = SparkContext(conf=conf)

# Input matrix and vector
matrix = [
    (0, [1, 2, 3]),
    (1, [4, 5, 6]),
    (2, [7, 8, 9])
]
vector = [2, 4, 6]

# Broadcast the vector to all nodes in the cluster
broadcast_vector = sc.broadcast(vector)

# Perform matrix-vector multiplication using MapReduce
result = sc.parallelize(matrix) \
    .map(lambda row: (row[0], sum([row[1][i] * broadcast_vector.value[i] for i in range(len(row[1]))]))) \
    .collect()

# Print the result
for row_id, value in sorted(result):
    print(f"Row {row_id}: {value}")

# Stop SparkContext
sc.stop()
