## Project Name: ST 554 Project 2
## Author: Franklin Zhou
## Create Date: Mar 8 2026
## Last Edit Date: Mar 8 2026
## Version: 0.1

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    
    def __init__(self, dataframe):
        self.df = dataframe
        
    ###
    # Add two @classmethods.
    ###
    
    @classmethods
    def read_csv(cls, spark, file_path): # three arguments for the class, spark session and path to the file
        csv_dataframe = spark.read.load(file_path,
                                 format="csv",
                                 sep=",",
                                 inferSchema="true",
                                 header="true")
        return cls(csv_dataframe)

    @classmethod
    def from_pandas_df(cls, spark, pandas_df): # three arguments for the class, the spark session, and the pandas dataframe
        pandas_dataframe = spark.createDataFrame(pandas_df)
        return cls(pandas_dataframe)
    
    ###
    # Validation methods
    ###
    
    def check_limits(self, column, lower, upper): 
                
        # If the user supplies a non-numeric column, print a message and return the df without modification.
        if dict(self.df.dtypes)[column] not in ['float', 'int', 'longint', 'bigint', 'double', 'integer']:
            print("Warning: Non-numeric column. Nothing is modified.")
            return self

        if lower is not None and upper is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", self.df[column].between(lower, upper))

        elif lower is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", self.df[column] >= lower)

        elif upper is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", self.df[column] <= upper)

        else:
            print("At least one bound must be provided")
            return self

        
        
        