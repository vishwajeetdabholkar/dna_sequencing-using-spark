// #### scala ####

val df = spark.read.option("header", "true").csv("dna_sequence.txt")


val df2 = df.withColumn("dna_class",
       when(col("dna_code") >= 100 && col("dna_code") < 25000, "A-DNA")
      .when(col("dna_code") >= 25000 && col("dna_code") < 100000, "B-DNA")
	  .when(col("dna_code") >= 100000 && col("dna_code") < 250000, "Z-DNA"))


val df3 = df2.groupBy("dna_class").count()

df2.write.parquet("dna_class")
df3.write.parquet("dna_count")
