import numpy as np
import time
import multiprocessing
import psutil  # For system monitoring



# Simulate large-scale dataset
data_size = 10000000
data = np.random.random(data_size)

# Function to simulate processing task
def process_data(data_chunk):
    # Perform some computation on the data
    result = np.mean(data_chunk)
    return result

# Function to dynamically allocate resources based on system metrics
def dynamic_resource_allocation():
    try:
      while True:
          # Get current system metrics
          cpu_percent = psutil.cpu_percent()  # CPU utilization percentage
          available_memory = psutil.virtual_memory().available  # Available memory in bytes

          # Define resource allocation based on system metrics
          if cpu_percent < 80 and available_memory > 2 * data_size * 16:  # Example thresholds
              num_processes = multiprocessing.cpu_count()  # Use all available cores
          else:
              num_processes = multiprocessing.cpu_count() // 2  # Use half of the available cores

          # Split data into chunks for parallel processing
          data_chunks = np.array_split(data, num_processes)

          # Process data chunks in parallel
          start_time = time.time()
          with multiprocessing.Pool(num_processes) as pool:
              results = pool.map(process_data, data_chunks)
          parallel_time = time.time() - start_time

          print(f"Parallel processing with {num_processes} processes took {parallel_time} seconds (CPU percentage {cpu_percent} )")

          # Sleep for a while before checking system metrics again
          time.sleep(10)  # Adjust as needed
    except KeyboardInterrupt:
        print("Program interrupted. Exiting gracefully...")
        # Additional cleanup if needed

# Main function
def main():
    dynamic_resource_allocation()

if __name__ == "__main__":
    main()


"""
"""