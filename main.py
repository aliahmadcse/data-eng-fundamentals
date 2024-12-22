from src.transformation import *


if __name__ == "__main__":
    movies_df = load_df(table_name="movies")
    ratings_df = load_df(table_name="ratings")
    joined_df = transform_avg_ratings(movies_df, ratings_df)
    load_df_to_db(joined_df)
