import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, first,min, count, expr, struct, collect_list, explode_outer,col, sum as spark_sum, max as spark_max
from pyspark.sql.window import Window

import sys
import os
PYSPARK_PYTHON = "/usr/local/bin/python3.10"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
def process(df, size):
    # Step 1: 获取每个 peer_id 包含 id_2 的年份
    year_by_peer_id = df.groupBy("peer_id").agg(
        first(when(col("peer_id").contains(col("id_2")), col("year"))).alias("years")
    )
    print(year_by_peer_id.show())


    # Step 2: 计算每个 peer_id 每年的数量，并按年份降序排列
    count_by_year = df.join(year_by_peer_id, ["peer_id"]) \
        .filter(col("year") <= col("years")) \
        .groupBy("peer_id", "year") \
        .count() \
        .orderBy(col("year").desc()) \
        .withColumnRenamed("year", "count_year")
    print(count_by_year.show())

    # Step 3: 检查是否有年份的数量满足给定大小，如果没有，则选择连续年份直到数量满足要求
    # 定义窗口规范
    window_spec = Window.partitionBy("peer_id").orderBy(col("count_year").desc())

    # 排序并计算累加和
    sorted_df = count_by_year.withColumn("cumulative_count", spark_sum("count").over(window_spec))

    filtered_df = sorted_df.filter(col("cumulative_count") >= size). \
        withColumn("max_year", spark_max("count_year").over(Window.partitionBy("peer_id"))). \
        filter(col("count_year") == col("max_year")). \
        drop("count_year")
    result_df = count_by_year.join(filtered_df, ["peer_id"]).filter(col("count_year") >= col("max_year")). \
        drop("cumulative_count").drop("max_year")
    print(result_df.show())

    # 关闭 SparkSession
    return result_df


if __name__ == "__main__":
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("PeerYearCount") \
        .getOrCreate()

    # 创建 DataFrame
    data = [
        ("ABC17969(AB)", "1", "ABC17969", 2022),
        ("ABC17969(AB)", "2", "CDC52533", 2022),
        ("ABC17969(AB)", "3", "DEC59161", 2023),
        ("ABC17969(AB)", "4", "F43874", 2022),
        ("ABC17969(AB)", "5", "MY06154", 2021),
        ("ABC17969(AB)", "6", "MY4387", 2022),
        ("AE686(AE)", "7", "AE686", 2023),
        ("AE686(AE)", "8", "BH2740", 2021),
        ("AE686(AE)", "9", "EG999", 2021),
        ("AE686(AE)", "10", "AE0908", 2021),
        ("AE686(AE)", "11", "QA402", 2022),
        ("AE686(AE)", "12", "OM691", 2022)
    ]
    df = spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

    # 处理数据
    result = process(df, 3)

    # 显示结果
    result.show()

    # 停止 SparkSession
    spark.stop()




