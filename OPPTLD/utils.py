import dask.dataframe as dd


def print_memory_usage(df):
    """
    Prints the memory usage of a Dask DataFrame.

    Args:
        df (dask.dataframe.DataFrame): The Dask DataFrame to analyze.
    """
    print(df.memory_usage())


def estimate_memory_usage(df):
    """
    Estimates the total memory usage of a Dask DataFrame in bytes.

    Args:
        df (dask.dataframe.DataFrame): The Dask DataFrame to analyze.

    Returns:
        int: Estimated memory usage in bytes.
    """
    return df.npartitions * df.head(n=1).memory_usage(deep=True).sum()


def measure_execution_time(func, *args, **kwargs):
    """
    Measures the execution time of a function.

    Args:
        func: The function to measure.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        float: Execution time in seconds.
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time
