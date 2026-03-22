class Transformations:
    def __init__(self, df=None):
        self.df = df

    def drop_cols(self, *col):
        for col in col:
            self.df = self.df.drop(col)
        return self.df

    def read_Streams(self, data_name, folder_name):
        self.df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.InferColumnTypes", "true") \
            .option("cloudFiles.schemaLocation", f"abfss://silver@trsdatalakejess.dfs.core.windows.net/{folder_name}/checkpoint_location") \
            .option("schemaEvolutionMode", "addNewColumns") \
            .load(f"abfss://bronze@trsdatalakejess.dfs.core.windows.net/{data_name}")
        return self.df

    def writeStream(self, data_name, folder_name):
        self.df.writeStream.format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"abfss://silver@trsdatalakejess.dfs.core.windows.net/{folder_name}/checkpoint_location") \
            .option("path", f"abfss://silver@trsdatalakejess.dfs.core.windows.net/{folder_name}/{data_name}") \
            .trigger(once=True) \
            .toTable(f"sales_project.silver.{data_name}")
        return self.df

    def drop_data(self, *col_name):
        for c in col_name:
            self.df = self.df.drop(c)
        return self.df

    def rename_cols(self, old_name: str, new_name: str):
        self.df = self.df.withColumnRenamed(old_name, new_name)
        return self.df

    def drop_duplicates(self, col_name):
        self.df = self.df.dropDuplicates(col_name)
        return self.df
    
    def cast_Int(self, col_names: list):
        for col_name in col_names:
            self.df = self.df.withColumn(col_name, self.df[col_name].cast(IntegerType()))
        return self.df

    def add_updatedtimestamp(self, column_name):
        self.df = self.df.withColumn(column_name, current_timestamp())
        return self.df
