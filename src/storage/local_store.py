def save_data_locally(data, filename, directory='data/raw'):
    import os
    import pandas as pd

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Create the full file path
    file_path = os.path.join(directory, filename)

    # Save the data to a CSV file
    data.to_csv(file_path, index=False)
    print(f"Data saved locally to {file_path}")

def load_data_locally(filename, directory='data/raw'):
    import os
    import pandas as pd

    # Create the full file path
    file_path = os.path.join(directory, filename)

    # Load the data from a CSV file
    if os.path.exists(file_path):
        data = pd.read_csv(file_path)
        print(f"Data loaded from {file_path}")
        return data
    else:
        print(f"File {file_path} does not exist.")
        return None