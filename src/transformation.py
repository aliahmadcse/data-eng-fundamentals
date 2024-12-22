import pyspark.sql
import random

spark = (
    pyspark.sql.SparkSession.builder
    .appName("Python Spark SQL basic example")
    .config('spark.driver.extraClassPath', "./libs/postgresql-42.7.4.jar")
    .getOrCreate()
)

READ_FORMAT = "jdbc"
JDBC_URL = "jdbc:postgresql://localhost:5432/etl_pipeline"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def load_df(table_name: str) -> pyspark.sql.DataFrame:
    return (
        spark.read
        .format(READ_FORMAT)
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .load()
    )


def transform_avg_ratings(movies_df: pyspark.sql.DataFrame, ratings_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    avg_rating = ratings_df.groupBy("movie_id").mean("rating")
    df = movies_df.join(
        avg_rating,
        movies_df.id == avg_rating.movie_id
    )
    df = df.drop("movie_id")
    return df


def load_df_to_db(df: pyspark.sql.DataFrame):
    df.write.jdbc(
        url=JDBC_URL,
        table="avg_ratings" + str(random.randint(1, 1000)),
        mode="overwrite",
        properties=JDBC_PROPERTIES
    )
