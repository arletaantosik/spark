class invoiceStreamBatch():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """

    def readInvoices(self):
        return (spark.readStream
                    .format("json")
                    .schema(self.getSchema())
                    .load(f"{self.base_data_dir}/data/invoices")
                )   

    def explodeInvoices(self, invoiceDF):
        return ( invoiceDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                      "DeliveryAddress.State","DeliveryAddress.PinCode", 
                                      "explode(InvoiceLineItems) as LineItem")
                                    )  
           
    def flattenInvoices(self, explodedDF):
        from pyspark.sql.functions import expr
        return( explodedDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                        .withColumn("ItemQty", expr("LineItem.ItemQty"))
                        .withColumn("TotalValue", expr("LineItem.TotalValue"))
                        .drop("LineItem")
                )
        
    def appendInvoices(self, flattenedDF, trigger = "batch"):
        sQuery = (flattenedDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices")
                    .outputMode("append")
                    .option("maxFilesPerTrigger", 1)   #every time only 1 file will be processed       
                    #by default it takes 1000 files, because ofter the files are very small
                )
        
        if (trigger == "batch"):
            return ( sQuery.trigger(availableNow = True) #3rd, takes all available data in the landing zone, if "maxFilesPerTrigger" = 1, and there's 10 files in the landing zone, it will be 10 micro-batches
                         .toTable("invoice_line_items"))
        else:
            return ( sQuery.trigger(processingTime = trigger) #2nd type of trigger
                         .toTable("invoice_line_items"))

    def process(self, trigger = "batch"):
           print(f"Starting Invoice Processing Stream...", end='')
           invoicesDF = self.readInvoices()
           explodedDF = self.explodeInvoices(invoicesDF)
           resultDF = self.flattenInvoices(explodedDF)
           sQuery = self.appendInvoices(resultDF, trigger)
           print("Done\n")
           return sQuery     
