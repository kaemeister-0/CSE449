import pandas as pd

def process_data(data_chunk):
    """
    This function performs various data analysis tasks on a chunk of data.

    Args:
        data_chunk (pd.DataFrame): A chunk of data from the large-scale dataset.

    Returns:
        pd.DataFrame: The processed data chunk.
    """

    # Data Cleaning (Replace with your specific cleaning steps)
    data_chunk.dropna(inplace=True)  # Remove rows with missing values

    # Descriptive Statistics
    print("Descriptive Statistics:")
    print(data_chunk.describe())  # Print summary statistics

    # Correlations
    print("Correlations:")
    print(data_chunk.corr())  # Calculate correlations between features

    # Feature Engineering (Replace with your specific transformations)
    data_chunk["new_feature"] = data_chunk["feature1"] * data_chunk["feature2"]  # Create a new feature

    # Data Visualization (Optional)
    # import matplotlib.pyplot as plt
    # plt.figure(figsize=(10, 6))
    # data_chunk["feature"].hist()  # Example histogram
    # plt.show()

    return data_chunk
