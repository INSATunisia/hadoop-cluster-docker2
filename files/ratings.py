import pandas as pd

merged_df = pd.read_csv('./merged_data.csv', )





# Save the merged data frame as a CSV file
merged_df.to_csv('ratings_.csv', index=False)
