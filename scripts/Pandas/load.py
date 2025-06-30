import pandas as pd

# Load: Save the cleaned data to a new CSV file
data = pd.read_csv('../data/refined.csv')

data.to_csv('../data/output.csv', index=False)
print("Data loaded to data/output.csv")