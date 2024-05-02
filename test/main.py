from logging import _levelToName
import time
import multiprocessing
import dask.dataframe as dd
from dask.distributed import Client
from distributed.diagnostics import MemorySampler

def process_data(data_chunk):
    # Perform analysis on the data chunk (e.g., calculations, transformations)
    # For demonstration, let's say we want to calculate the total number of customers in the chunk
    num_customers = len(data_chunk)
    
    # Example transformation: Convert country names to uppercase
    data_chunk['Country'] = data_chunk['Country'].str.upper()
    
    # Example calculation: Calculate the average length of the customer's full name
    data_chunk['Full Name'] = data_chunk['First Name'] + ' ' + data_chunk['Last Name']
    data_chunk['Full Name Length'] = data_chunk['Full Name'].apply(len)
    avg_name_length = data_chunk['Full Name Length'].mean()
    
    # Return processed chunk or results
    return {
        'num_customers': num_customers,
        'avg_name_length': avg_name_length,
        'processed_chunk': data_chunk
    }

def compare_in_memory_vs_on_disk(data_path):
    # Load data into memory
    start_time = time.time()
    data_in_memory = dd.read_csv(data_path)
    end_time = time.time()
    in_memory_time = end_time - start_time

    # Process data in memory using parallel processing
    start_time = time.time()
    processed_data_in_memory = data_in_memory.map_partitions(process_data)
    end_time = time.time()
    in_memory_processing_time = end_time - start_time

    # Process data on disk using Dask
    start_time = time.time()
    data_on_disk = dd.read_csv(data_path, blocksize=None)  # Read entire file as one chunk
    processed_data_on_disk = data_on_disk.map_partitions(process_data)
    end_time = time.time()
    on_disk_processing_time = end_time - start_time

    # Analyze memory usage
    memory_usage_in_memory = data_in_memory.memory_usage()
    memory_usage_on_disk = processed_data_on_disk.memory_usage()

    # Print results
    print("In-memory processing time:", in_memory_processing_time)
    print("On-disk processing time:", on_disk_processing_time)
    print("In-memory memory usage:", memory_usage_in_memory)
    print("On-disk memory usage:", memory_usage_on_disk)

def explore_on_demand_computational_models(data_path, num_workers, memory_target_fraction, memory_limit):
    # Initialize Dask client with desired number of workers
    client = Client(n_workers=num_workers, memory_limit=memory_limit)
    
    # Load data using Dask
    data = dd.read_csv(data_path)

    # Process data using Dask's parallel execution
    processed_data = data.map_partitions(process_data)

    # Collect results and measure time
    start_time = time.time()
    results = processed_data.compute()
    end_time = time.time()
    on_demand_processing_time = end_time - start_time

    # Analyze memory usage
    
    memory_usage = processed_data.memory_usage()

    # Print results
    print("On-demand processing time with", num_workers, "workers:", on_demand_processing_time)
    print("On-demand memory usage:", memory_usage)


if __name__ == "__main__":
    data_path = "test\customers-100.csv"  # Replace with your actual data path
    num_workers = 6
    memory_target_fraction = 0.95
    memory_limit = '2GB'
    # Compare in-memory vs. on-disk processing
    compare_in_memory_vs_on_disk(data_path)

    # Explore on-demand computational models
    explore_on_demand_computational_models(data_path, num_workers, memory_target_fraction, memory_limit)  # Adjust worker count as needed
