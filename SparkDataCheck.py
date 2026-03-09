## Project Name: ST 554 Project 2
## Author: Franklin Zhou
## Create Date: Mar 8 2026
## Last Edit Date: Mar 8 2026
## Version: 0.1

# 
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    
    def __init__(self, dataframe):
        self.df = dataframe
    
    @classmethods
    def read_csv(cls, spark, file_path):
        csv_dataframe = spark.read.load(file_path,
                                 format="csv",
                                 sep=",",
                                 inferSchema="true",
                                 header="true")
        return cls(csv_df)

    @classmethod
    def from_pandas_df(cls, spark, pandas_df):
        pandas_dataframe = spark.createDataFrame(pandas_df)
        return cls(pandas_dataframe)