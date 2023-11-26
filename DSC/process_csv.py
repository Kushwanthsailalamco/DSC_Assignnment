import pandas as pd

# Read the uploaded CSV file
df = pd.read_csv("Test.csv")

# Update the 'GENDER' column to have only the first letter
df['GENDER'] = df['GENDER'].str[0]

# Display the updated DataFrame
print(df)

# Save the DataFrame back to a CSV file with the updated 'GENDER' column
df.to_csv("output.csv", index=False)
