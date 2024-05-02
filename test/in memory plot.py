import matplotlib.pyplot as plt

# Data
n_values = [1, 2, 3, 4]
in_memory_processing_times = [0.002999544143676758, 0.002000093460083008, 0.0029993057250976562, 0.002999544143676758]

# Plot
plt.figure(figsize=(8, 5))

plt.plot(n_values, in_memory_processing_times, marker='o', color='blue', label='In-memory processing time')

plt.xlabel('n')
plt.ylabel('Time (seconds)')
plt.title('In-memory Processing Time vs. n')
plt.legend()
plt.grid(True)

plt.show()