import time
from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext("local", "ParallelAlgorithms")

# Define process_data function (for demonstration purposes)
def process_data(data):
    return data * 2

# Define your data (or pass it as a parameter to the functions)
data = sc.parallelize(range(1000000))  # Example data

# 1. MapReduce
def map_reduce_example(data):
    # Map phase: Process each element in parallel
    mapped_data = data.map(process_data)

    # Reduce phase: Aggregate results
    reduced_result = mapped_data.reduce(lambda x, y: x + y)

    return reduced_result

# 2. Parallel Sorting
def parallel_sorting_example(data):
    sorted_data = data.sortBy(lambda x: x)
    return sorted_data.collect()

# 3. Parallel Search
def parallel_search_example(data, target):
    # Perform parallel search
    result = data.filter(lambda x: x == target).collect()
    return result

# 4. Parallel Join
def parallel_join_example(data1, data2):
    # Example datasets
    data1 = sc.parallelize([(1, 'A'), (2, 'B'), (3, 'C')])
    data2 = sc.parallelize([(1, 'X'), (2, 'Y'), (4, 'Z')])

    # Perform parallel join
    joined_data = data1.join(data2)
    return joined_data.collect()

# 5. Parallel Aggregation
def parallel_aggregation_example(data):
    aggregated_result = data.reduce(lambda x, y: x + y)
    return aggregated_result

# 6. Parallel Graph Algorithms (e.g., Parallel BFS)
def parallel_bfs_example(graph, start_node):
    # Perform parallel BFS
    visited = []
    queue = [start_node]

    while queue:
        node = queue.pop(0)
        if node not in visited:
            visited.append(node)
            neighbors = graph[node]
            queue.extend(neighbors)

    return visited

# 7. Parallel Machine Learning Algorithms (e.g., Parallel k-means)
def parallel_kmeans_example(data, k):
    # Perform parallel k-means clustering
    # (Note: This is a simplified example)
    centroids = data.takeSample(False, k)
    # Iterate until convergence
    # Implement k-means clustering algorithm here
    return centroids

# Example usage
print("MapReduce Example:", map_reduce_example(data))
print("Parallel Sorting Example:", parallel_sorting_example(data))
print("Parallel Search Example:", parallel_search_example(data, 42))
print("Parallel Join Example:", parallel_join_example(data, data))
print("Parallel Aggregation Example:", parallel_aggregation_example(data))
print("Parallel BFS Example:", parallel_bfs_example({1: [2, 3], 2: [4, 5], 3: [], 4: [], 5: []}, 1))
print("Parallel k-means Example:", parallel_kmeans_example(data, 3))

# Stop Spark context
sc.stop()