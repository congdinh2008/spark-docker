# apps/pi_calculation.py
import random
from pyspark.sql import SparkSession

def calculate_pi(partitions, spark):
    n = 100000 * partitions
    def f(_):
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(lambda x, y: x + y)
    print(f"Pi is roughly {4.0 * count / n}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Pi Calculation") \
        .getOrCreate()

    calculate_pi(10, spark) # Using 10 partitions
    spark.stop()