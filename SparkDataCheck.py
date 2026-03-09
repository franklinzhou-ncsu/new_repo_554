## Project Name: ST 554 Project 2
## Author: Franklin Zhou
## Create Date: Mar 8 2026
## Last Edit Date: Mar 9 2026
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
    
    # Validate numeric type
    def check_numeric(self, column, lower = None, upper = None):    
        # If the user supplies a non-numeric column, print a message and return the df without modification.
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        if dict(self.df.dtypes)[column] not in num_types:
            print("Warning: Non-numeric column. Nothing is modified.")
            return self

        if lower is not None and upper is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", F.when(self.df[column].isNull(), None).otherwise(self.df[column].between(lower, upper))) # if column value is empty, assign None        
        elif lower is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", F.when(self.df[column].isNull(), None).otherwise(self.df[column] >= lower))   
        elif upper is not None:
            self.df = self.df.withColumn("Numeric_Check_Result", F.when(self.df[column].isNull(), None).otherwise(self.df[column] <= upper))        
        else:
            print("Warning: At least one bound must be provided. Nothing is modified.")
            return self
        
        return self

    # Validate string type
    def check_string(self, column = None, levels = None):
        
        if dict(self.df.dtypes)[column] != "string":
            print("Warning: The column is not string. Nothing is modified.")
            return self        
        else:
            self.df = self.df.withColumn("String_Check_Result", F.when(self.df[column].isNull(), None).otherwise(self.df[column].isin(levels)))
            return self
        
    # Checking Missing values
    def check_missing(self, column):
        
        self.df = self.df.withColumn("Missing_Check_Result", self.df[column].isNull())
        
    ###
    # Summarization methods
    ###
    
    # find min and max of column(s)
    def min_max(self, column, group):
        # numerical types
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        # if column value is provided
        if column is not None:
            
            #check datatype first
            if dict(self.df.dtypes)[column] not in num_types:
                print("Warning: Non-numeric column. Nothing is returned.")
                return None
            
            # if group value is provided, find min and max of the column by that group
            if group:
                result = self.df.groupBy(group).agg(F.min(column), F.max(column)).toPandas()
                return result
                
            # if group value is not provided, 
            else:
                result = self.df.agg(F.min(column), F.max(column)).toPandas()
                return result
            
        # if column value is not provided, gives all numeric cloumns min and max
        else:
            # create list that has all numeric type columns
            numeric_cols = [c for c, t in self.df.dtypes if t in num_types]
            # save result to this list
            results = [] 
            
            for col in numeric_cols:
                if group:
                    result = self.df.groupBy(group).agg(F.min(col), F.max(col)).toPandas()
                else:
                    result = self.df.agg(F.min(col), F.max(col)).toPandas()
                results.append(result)
                
            combined = reduce(lambda left, right: pd.merge(left, right), results)
            return combined
            
    # medthod for counts
    def levels_count(self, col_1, col_2 = None):
        
        # if col_1 is not string, stop
        if dict(self.df.dtypes)[col_1] != "string":
            print("Warning: Col_1 is a numeric column. Nothing is returned.")
            return None
         # if col_1 is string:
        else:
            # if col_2 is not None
            if col_2:
                # if col_2 is not string, stop
                if dict(self.df.dtypes)[col_2] != "string":
                    print("Warning: Col_2 is a numeric column. Nothing is returned.")
                    return None
                # if col_2 is numeric, process
                else:
                    count = self.df.groupBy(col_1, col_2).count().toPandas()
            # if col_2 is None        
            else: 
                count = self.df.groupBy(col_1).count().toPandas()
            return count