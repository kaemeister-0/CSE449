import time
import multiprocessing
import dask.dataframe as dd
from dask.distributed import Client

# Define data processing function (replace with your actual analysis logic)
def process_data(data_chunk):
    # Perform analysis on the data chunk (e.g., calculations, transformations)
    return processed_chunk

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

def explore_on_demand_computational_models(data_path, num_workers):
    # Initialize Dask client with desired number of workers
    client = Client(n_workers=num_workers)

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
    data_path = "your/data/path.csv"  # Replace with your actual data path

    # Compare in-memory vs. on-disk processing
    compare_in_memory_vs_on_disk(data_path)

    # Explore on-demand computational models
    explore_on_demand_computational_models(data_path, num_workers=4)  # Adjust worker count as needed


import threading

var1 = 1
var2 = 2

def region1():
    global var1, var2
    print("Region 1: var1={}, var2={}".format(var1, var2))
    var1 += 1
    var2 += 1

def region2(var1_copy, var2_copy):
    print("Region 2: var1={}, var2={}".format(var1_copy, var2_copy))
    var1_copy += 1
    var2_copy += 1

def region3():
    global var1, var2
    print("Region 3: var1={}, var2={}".format(var1, var2))
    var1 += 1
    var2 += 1

# Region 1: private variables
thread1 = threading.Thread(target=region1)
thread1.start()
thread1.join()
print("After region 1: var1={}, var2={}\n".format(var1, var2))

# Region 2: firstprivate variables
thread2 = threading.Thread(target=region2, args=(var1, var2))
thread2.start()
thread2.join()
print("After region 2: var1={}, var2={}\n".format(var1, var2))

# Region 3: shared variables
thread3 = threading.Thread(target=region3)
thread3.start()
thread3.join()
print("After region 3: var1={}, var2={}\n".format(var1, var2))

###RC
import numpy as np
from numba import njit, prange # type: ignore

NX = 102400

@njit(parallel=True)
def compute_sum(vecA):
    sum = 0
    for i in prange(NX):
        sum += vecA[i]
    return sum

def main():
    vecA = np.arange(1, NX + 1, dtype=np.int64)
    sumex = NX * (NX + 1) // 2
    print("Arithmetic sum formula (exact):", sumex)

    sum = compute_sum(vecA)
    print("Sum:", sum)

if __name__ == "__main__":
    main()


##
from mpi4py import MPI
from numba import njit, prange

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

@njit(parallel=True)
def process_data(rank, size):
    tid = 0
    nthreads = 1
    msg = 0

    if rank == 0:
        nthreads = 1  # placeholder value for the number of threads in master rank
        print("%i threads in master rank" % nthreads)
        for i in range(1, size):
            comm.send(tid, dest=i, tag=tid)
    else:
        msg = comm.recv(source=0, tag=rank)
        print("Rank %i thread %i received %i" % (rank, tid, msg))

def main():
    process_data(rank, size)

if __name__ == "__main__":
    main()