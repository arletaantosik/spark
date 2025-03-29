class batchWC():
    def __init__(self): #class constructor
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (spark.read #lines dataframe
                    .format("text")
                    .option("lineSep", ".")
                    .load(f"{self.base_data_dir}/data/text")
                )
        return lines.select(explode(split(lines.value, " ")).alias("word")) #converting lines to words
    
    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim, lower
        return ( rawDF.select(lower(trim(rawDF.word)).alias("word"))
                        .where("word is not null")
                        .where("word rlike '[a-z]'") #word starts with 'a-z'
                )
        
    def getWordCount(self, qualityDF):
        return qualityDF.groupBy("word").count()
    #creates aggregated result
  
    def overwriteWordCount(self, wordCountDF):
        ( wordCountDF.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("word_count_table")
        )
    
    def wordCount(self):
        print(f"\tExecuting Word Count...", end='')
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.overwriteWordCount(resultDF)
        print("Done")

batch = batchWC()
batch.wordCount()
