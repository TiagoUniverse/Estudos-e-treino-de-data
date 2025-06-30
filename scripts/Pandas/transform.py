import pandas as pd

# Transform: For example, remove rows with missing values and create a new column
data = pd.read_csv('../data/input.csv')

data_clean = data.dropna()

data_clean.to_csv('../data/refined.csv', index=False)
print("Data loaded to data/refined.csv")