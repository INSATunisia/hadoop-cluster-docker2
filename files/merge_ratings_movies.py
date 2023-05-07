import pandas as pd

# Load movies and ratings data into separate data frames
movies_df = pd.read_csv('./ml-latest-small/movies.csv', )
ratings_df = pd.read_csv('./ml-latest-small/ratings.csv')

# Merge the two data frames based on the movieId column
merged_df = pd.merge(movies_df, ratings_df, on='movieId')



# Save the merged data frame as a CSV file
merged_df.to_csv('merged_data.csv', index=False)
