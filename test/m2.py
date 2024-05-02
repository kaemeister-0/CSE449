import time
import multiprocessing as mp
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import pandas as pd

# Function to perform data analysis on a chunk of data
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
        'analyze_chunk': data_chunk
    }

'''def analyze_chunk(data_chunk):
    """
    This function analyzes a chunk of data.

    Parameters:
    data_chunk (any type): The chunk of data to be analyzed. The type of data_chunk would depend on the context.

    Returns:
    any type: The result of the analysis. The type of the return value would depend on the specific analysis being done.
    """
    # Analysis code here
    pass'''
# Function to measure performance and memory consumption
def measure_performance(data, method, num_workers):
    start_time = time.time()

    if method == 'in_memory':
        # Load data into memory
        data_in_memory = data.copy()  # Avoid modifying original data
        processed_data = data_in_memory.apply(analyze_chunk, axis=1)
    elif method == 'on_disk':
        # Process data in chunks directly from disk using Dask
        dask_client = Client(LocalCluster(n_workers=num_workers))  # Create Dask client
        dask_df = dd.from_pandas(data, npartitions=num_workers)  # Create Dask DataFrame
        processed_data = dask_df.apply(analyze_chunk, axis=1)
    else:
        raise ValueError("Invalid processing method: {}".format(method))

    end_time = time.time()
    processing_time = end_time - start_time

    # Memory consumption measurement (example using psutil)
    import psutil
    memory_usage = psutil.virtual_memory().used / (1024 * 1024 * 1024)  # Convert to GB

    return processing_time, memory_usage

# Main research loop for experimentation
def main():
    # Define data path and analysis parameters
    data_path = 'test\customers-10000.csv'  # Replace with your actual data path
    num_workers = 4  # Adjust based on your hardware and data size
    analysis_iterations = 10  # Number of iterations for each experiment

    # Experiment with different data processing methods
    for method in ['in_memory', 'on_disk']:
        for _ in range(analysis_iterations):
            data = pd.read_csv(data_path)  # Read data into pandas DataFrame

            processing_time, memory_usage = measure_performance(data, method, num_workers)

            # Print results for each iteration
            print("Method:", method, "Workers:", num_workers)
            print("Processing Time:", processing_time, "seconds")
            print("Memory Usage:", memory_usage, "GB")

if __name__ == '__main__':
    main()
