from pyspark import SparkContext
from math import sqrt

# Dummy input data
input_data = [
    'key1\t10',
    'key2\t20',
    'key1\t30',
    'key2\t40',
    'key1\t50',
    'key2\t60',
]

def map_func(line):
    key, value = line.split('\t')
    return key, float(value)

def reduce_func(data):
    values = [x for x in data]
    mean_val = sum(values) / len(values)
    sum_val = sum(values)
    std_dev_val = sqrt(sum((x - mean_val)**2 for x in values) / (len(values) - 1)) if len(values) > 1 else 0
    return {
        'mean': mean_val,
        'sum': sum_val,
        'std_dev': std_dev_val
    }

if __name__ == '__main__':
    sc = SparkContext('local', 'AggregationSpark')
    lines = sc.parallelize(input_data)
    mapped = lines.map(map_func)
    grouped = mapped.groupByKey()
    result = grouped.mapValues(list).mapValues(reduce_func)
    output = result.collect()
    for key, value in output:
        print(f'{key}\t{value}')
    sc.stop()
