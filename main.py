from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def top5():
    filepath = "objets-trouves-restitution.csv"
    spark = SparkSession.builder.appName("AnalyseData").getOrCreate()
    df = spark.read.option("header", "true").option("sep", ";").csv(filepath)
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.replace(" ", "_").replace(";", "").replace("'", ""))
    top_5_villes = df.groupBy("Gare").count().orderBy(col("count").desc()).limit(5)
    top_5_villes.show()
    top_1_nature = df.groupby("Nature_dobjets").count().orderBy(col("count").desc()).limit(1)
    top_1_nature.show()

if __name__ == '__main__':
    top5()


