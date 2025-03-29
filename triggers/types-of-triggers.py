# 1. unspecified
  #micro-batches will be generated as soon as the previous micro-batch has completed processing 
  #ASAP-seconds

  df.writeStream \
    .format("delta")  \
    .option("checkpointLocation", "/chkpt/dir") \
    .toTable("table_name")

# 2. fixed interval
  #micro-batches will be kicked off at user-specified intervals
  #collect and process - minutes
  #when the whole process ends in 20 seconds, but we select 30 seconds, it will wait additional 10 seconds before processing another batch of data

  df.writeStream \
    .format("delta")  \
    .option("checkpointLocation", "/chkpt/dir") \
    .trigger(processingTime='30 seconds') \
    .toTable("table_name")

# 3. available-now
  # one-time micro-batch trigger to process all the available data and then stop on its own 
  # incremental batch - hours
  # takes all data available in the landing zone, executes one micro batch, finishes and then streaming query automatically ends
  # if we want to run a trigger every one hour, we have to schedule an application to start every hour
  # more cost efficiency than the 2nd approach -> if we set that in 2nd approach, even the processing ends in 2 minutes, the resources are being used even if the batch is waiting for another 58 minutes

  df.writeStream \
    .format("delta")  \
    .option("checkpointLocation", "/chkpt/dir") \
    .trigger(availableNow=True) \ 
    .toTable("table_name")
