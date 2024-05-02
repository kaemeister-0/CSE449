'''import matplotlib.pyplot as plt

# Data
n_values = [1, 2, 3, 4]  # Update with your actual n values
workers = [1, 2, 3, 4]   # Update with the number of workers used
ondemand_processing_times = [4.667253017425537, 3.1490025520324707, 3.1560075283050537, 3.133014678955078]  # Update with your actual on-demand processing times

# Plot
plt.plot(workers, ondemand_processing_times, marker='o')
plt.title('On-demand Processing Time vs. Number of Workers')
plt.xlabel('Number of Workers')
plt.ylabel('On-demand Processing Time (seconds)')
plt.xticks(workers)
plt.grid(True)
plt.show()'''

import matplotlib.pyplot as plt

# Data
n_values = [1, 2, 3, 4]
in_memory_processing_times = [0.002999544143676758, 0.002000093460083008, 0.0029993057250976562, 0.002999544143676758]
on_disk_processing_times = [0.006000518798828125, 0.0070002079010009766, 0.005000591278076172, 0.0070002079010009766]
in_memory_memory_usage = ["Dask Series Structure: npartitions=1 int64 ... dtype: int64", "Dask Series Structure: npartitions=1 int64 ... dtype: int64", "Dask Series Structure: npartitions=1 int64 ... dtype: int64", "Dask Series Structure: npartitions=1 int64 ... dtype: int64"]
on_disk_memory_usage = ["dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>"]
on_demand_processing_times = [4.667253017425537, 3.1490025520324707, 3.1560075283050537, 3.133014678955078]
on_demand_memory_usage = ["dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>", "dd.Scalar<series-..., dtype=int64>"]

# Plot
plt.figure(figsize=(10, 6))

plt.plot(n_values, in_memory_processing_times, marker='o', label='In-memory processing time')
plt.plot(n_values, on_disk_processing_times, marker='o', label='On-disk processing time')
plt.plot(n_values, on_demand_processing_times, marker='o', label='On-demand processing time')

plt.xlabel('n')
plt.ylabel('Time (seconds)')
plt.title('Processing Time vs. n')
plt.legend()
plt.grid(True)

plt.show()
