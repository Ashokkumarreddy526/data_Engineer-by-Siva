# Databricks notebook source
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/landing_zone/vessel/agent_details/agent_details.csv")

df.display()


# COMMAND ----------

from pyspark.sql.functions import col, when

df2 = df.withColumn(
    "role",
    when(col("agent_type") == 0, "owners")
    .when(col("agent_type") == 1, "charters")
)

df2.display()

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import col, when, concat, lit

df3 = df2.withColumn(
    "role_details",
    when(
        col("role").isNotNull() & col("agent_name").isNotNull(),
        concat(col("role"), lit(" - "), col("agent_name"))
    ).when(
        col("agent_name").isNotNull(),
        concat(col("agent_name"),lit(""))
    )
    )


display(df3)


# COMMAND ----------

from pyspark.sql.functions import collect_list, concat_ws

df_agg = df3.groupBy("agent").agg(
    concat_ws(" | ", collect_list("role_details")).alias("details")
)

df_agg.display()


# COMMAND ----------

df_final = df_agg.withColumn(
    "agent_details",
    concat(col("agent"), lit(" : "), col("details"))
)

df_final.display()

# COMMAND ----------

df_final.write.mode("overwrite").option("header", "true").csv("/Volumes/curated_zone/vessel/agent_details/agent_details")
