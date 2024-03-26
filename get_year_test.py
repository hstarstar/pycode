import unittest

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from get_year import *


class TestProcess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 初始化 SparkSession
        cls.spark = SparkSession.builder \
            .appName("TestProcess") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # 停止 SparkSession
        cls.spark.stop()

    def test_year_by_peer_id(self):
        # 输入数据
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
        df = self.spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

        # 获取每个 peer_id 包含 id_2 的年份
        result_df = process(df, 3)

        # 检查结果 DataFrame 是否符合预期
        expected_data = [
            ("ABC17969(AB)", 2022),
            ("AE686(AE)", 2023)
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["peer_id", "years"])

        self.assertTrue(expected_df.collect() == result_df.collect())

    def test_count_by_year(self):
        # 输入数据
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
        df = self.spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

        # 计算每个 peer_id 每年的数量
        result_df = process(df, 3)

        # 检查结果 DataFrame 是否符合预期
        expected_data = [
            ("ABC17969(AB)", 2023, 1),
            ("ABC17969(AB)", 2022, 3),
            ("ABC17969(AB)", 2021, 2),
            ("AE686(AE)", 2023, 1),
            ("AE686(AE)", 2022, 2),
            ("AE686(AE)", 2021, 3)
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["peer_id", "count_year", "count"])

        self.assertTrue(expected_df.collect() == result_df.collect())

    def test_process(self):
        # 输入数据
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
        df = self.spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

        # 处理数据
        result_df = process(df, 3)

        # 检查结果 DataFrame 是否符合预期
        expected_data = [
            ("ABC17969(AB)", 2023, 1),
            ("ABC17969(AB)", 2022, 3),
            ("AE686(AE)", 2023, 1),
            ("AE686(AE)", 2022, 2),
            ("AE686(AE)", 2021, 3)
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["peer_id", "count_year", "count"])

        self.assertTrue(expected_df.collect() == result_df.collect())


if __name__ == "__main__":
    unittest.main()
