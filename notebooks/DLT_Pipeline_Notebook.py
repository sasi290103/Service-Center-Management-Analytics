# Databricks notebook source
# MAGIC %md
# MAGIC ### loading data ###
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog service_center

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="customer data"
)
def bronze_customer():
    df=spark.read.table("service_center.bronze_schema.customer_data")
    return df


# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="sales data"
)
def bronze_sales():
    df=spark.read.table("service_center.bronze_schema.sales_data")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="branch conveniance score"
)
def bronze_branch_conveniance_score():
    df=spark.read.table("service_center.bronze_schema.branch_conveniance_score")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="service center6 raw data"
)
def bronze_service_center():
    df=spark.read.table("service_center.bronze_schema.service_centre")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="sucess score"
)
def bronze_sucess_score():
    df=spark.read.table("service_center.bronze_schema.success_score")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"bronze"},
    comment="vehcile data"
)
def bronze_vehicle():
    df=spark.read.table("service_center.bronze_schema.vehicle_data")
    return df   

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"silver"},
    comment="customer data"
)
@dlt.expect_or_drop("customer_id is not null", "customer_id IS NOT NULL")
def silver_customer():
    df=dlt.read("bronze_customer")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"silver"},
    comment="sales data"
)
@dlt.expect_or_drop("valid_chassis", "Chassis_Number IS NOT NULL")
def silver_sales():
    df=dlt.read("bronze_sales")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"silver"},
    comment="silver service center"
)
@dlt.expect_or_drop("cutomer_id is not null", "customer_id IS NOT NULL")
def service_center_silver():
    df=dlt.read("bronze_service_center")
    return df

# COMMAND ----------

import dlt

@dlt.view(
    comment="customer sales"
)
def customer_sales():
    df1 = dlt.read("silver_customer").select("Chassis_Number","Location")
    df2 = dlt.read("silver_sales").select(
        "Chassis_Number",
        "Branch_id",
        "Vehicle_ID",
        "Model",
        "Temporary_Registration_Number",
        "Invoice_Date",
        "Tags",
        "Final_Price",
        "customer_id"
    )
    
    # Inner join on Chassis_Number and select relevant fields
    df_final = df2.join(df1, df1.Chassis_Number == df2.Chassis_Number, "inner") \
                  .select(
                      df2.Chassis_Number,
                      df2.Branch_id,
                      df2.Vehicle_ID,
                      df2.Model,
                      df2.Temporary_Registration_Number,
                      df2.Invoice_Date,
                      df2.Tags,
                      df2.Final_Price,
                      df2.customer_id,
                      df1.Location
                  )
    return df_final


# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

@dlt.table(
     table_properties={"quality":"gold"},
     comment="sales agrregated branch wise"
)
def gold_sales_branch_wise():
     df=dlt.read("customer_sales")
     df_final=df.groupBy("Branch_id").agg(sum("Final_Price").alias("total_sales_branchwise"))
     return df_final

# COMMAND ----------

from pyspark.sql.functions import col
import dlt

@dlt.view(
    comment="not visited service center"
)
def customer_not_visited():
    df1 = dlt.read("silver_customer").select("Chassis_Number", "customer_id", "Branch_id")
    df2 = dlt.read("service_center_silver").select("Chassis_Number")
    
    df_final = df1.join(
        df2,
        df1["Chassis_Number"] == df2["Chassis_Number"],
        "left_anti"
    ).select(
        df1.Chassis_Number,
        df1.customer_id,
        df1.Branch_id
    )
    
    return df_final


# COMMAND ----------

import dlt
from pyspark.sql.functions import col

@dlt.view(
    comment="Customer and service center merged data"
)
def customer_service_center():
    df1 = dlt.read("silver_customer").select("Chassis_Number", "Invoice_Date","Location","Customer_Name","Email_ID")
    
    df2 = dlt.read("service_center_silver").select(
        "Chassis_Number",
        "customer_id",
        "Branch_ID",
        "Model",
        "Speciality",
        "Parts_Added",
        "Total_Price",
        "Previous_Service_Date"
    )
    
    df_final = df2.join(
        df1,
        df2["Chassis_Number"] == df1["Chassis_Number"],
        "inner"
    ).select(
        df2["Chassis_Number"],
        df2["customer_id"],
        df2["Branch_ID"],
        df2["Model"],
        df2["Speciality"],
        df2["Parts_Added"],
        df2["Total_Price"],
        df1["Invoice_Date"],
        df2["Previous_Service_Date"],
        df1["Location"],
        df1["Customer_Name"],
        df1["Email_ID"]
    )
    return df_final

# COMMAND ----------

# from pyspark.sql.functions import col, datediff, current_timestamp

# @dlt.table(
#     comment="Customers who have not visited the service center in the last 120 days"
# )
# def not_visited_120_days():
#     df = dlt.read("customer_service_center")

#     df = df.withColumn("Previous_Service_Date", col("Previous_Service_Date").cast("timestamp")) \
#            .select("Previous_Service_Date", "Customer_Name", "Email_ID", "Branch_ID", "customer_id")

#     df_filtered = df.filter(
#         datediff(current_timestamp(), col("Previous_Service_Date")) > 120
#     )

#     df_result = df_filtered.dropna().distinct()

#     return df_result


# COMMAND ----------

from pyspark.sql.functions import col, datediff, current_date, max as spark_max

@dlt.table(
    comment="Customers who have visited the service center in the last 120 days"
)
def visited_120_days():
    df = dlt.read("customer_service_center")

    df = df.withColumn("Previous_Service_Date", col("Previous_Service_Date").cast("date")) \
           .select("customer_id", "Customer_Name", "Email_ID", "Branch_ID", "Previous_Service_Date")

    latest_service = df.groupBy("customer_id").agg(
        spark_max("Previous_Service_Date").alias("Last_Service_Date")
    )

    inactive_customers = latest_service.filter(
        datediff(current_date(), col("Last_Service_Date")) <= 120
    )

    final_result = inactive_customers.join(df, on="customer_id", how="left") \
                                     .select("customer_id", "Customer_Name", "Email_ID", "Branch_ID", "Last_Service_Date") \
                                     .dropDuplicates(["customer_id"])

    return final_result


# COMMAND ----------

from pyspark.sql.functions import col, datediff, current_date, max as spark_max

@dlt.table(
    comment="Customers who have not visited the service center in the last 120 days"
)
def not_visited_120_days():
    df = dlt.read("customer_service_center")

    df = df.withColumn("Previous_Service_Date", col("Previous_Service_Date").cast("date")) \
           .select("customer_id", "Customer_Name", "Email_ID", "Branch_ID", "Previous_Service_Date","Model")

    latest_service = df.groupBy("customer_id").agg(
        spark_max("Previous_Service_Date").alias("Last_Service_Date")
    )

    inactive_customers = latest_service.filter(
        datediff(current_date(), col("Last_Service_Date")) > 120
    )

    final_result = inactive_customers.join(df, on="customer_id", how="left") \
                                     .select("customer_id", "Customer_Name", "Email_ID", "Branch_ID", "Last_Service_Date","Model") \
                                     .dropDuplicates(["customer_id"])

    return final_result


# COMMAND ----------

# from pyspark.sql.functions import col, datediff, current_timestamp

# @dlt.table(
#     comment="Customers who have not visited the service center in the last 120 days"
# )
# def not_visited_120_days():
#     df = dlt.read("customer_service_center")

#     df = df.withColumn("Previous_Service_Date", col("Previous_Service_Date").cast("timestamp")) \
#            .select("Previous_Service_Date", "Customer_Name", "Email_ID", "Branch_ID", "customer_id")

#     df_filtered = df.filter(
#         datediff(current_timestamp(), col("Previous_Service_Date")) > 120
#     )

#     df_result = df_filtered.dropna().distinct()

#     return df_result


# COMMAND ----------

# @dlt.table(
#     comment="Customers who not visited within 120 days"
# )
# def not_visited_120_days():
#     df = dlt.read("customer_service_center")
#     df = df.withColumn("Previous_Service_Date", col("Previous_Service_Date").cast("timestamp"))
#     df_final = df.filter(
#         datediff(current_timestamp(), col("Previous_Service_Date")) > 120
#     )
#     df_final=df_final.groupBy("Branch_ID","customer_id").agg(sum("Branch_ID").alias("sum"))
#     df_final =df_final.select("Branch_ID", "customer_id").dropna()
    
    
#     return df_final

# COMMAND ----------

# @dlt.table(
#     comment="Visited_120days_Customer_Details"
# )
# def gold_Visited_120days_Customer_Details():
#     df=dlt.read("visited_120_days")
#     final_df=df.groupBy("Branch_ID","customer_id").agg(sum("Branch_ID").alias("sum"))
#     final_df = final_df.select("Branch_ID", "customer_id").dropna()
#     return final_df

# COMMAND ----------

# @dlt.table(
#     comment="not_visited_120_days_customer_details"
# )
# def gold_not_visited_120_days_customer_details():
#     df=dlt.read("not_visited_120_days")
#     final_df=df.groupBy("Branch_ID","customer_id").agg(sum("Branch_ID").alias("sum"))
#     final_df = final_df.select("Branch_ID", "customer_id").dropna()
#     return final_df

# COMMAND ----------

@dlt.table(
    table_properties={"quality":"silver"},
    comment="silver_success_score"
)
def silver_success_score():
    df=dlt.read("bronze_sucess_score")
    return df

# COMMAND ----------

@dlt.table(
    table_properties={'quality':'gold'},
    comment="gold_success_score"
)
def gold_success_score():
    df=dlt.read("silver_success_score")
    df_final=df.groupBy(["Branch_ID","Model","Branch_Convenience_Score"]).agg(sum("Branch_Convenience_Score").alias("overall_score"))
    return df_final

# COMMAND ----------

@dlt.table(
    table_properties={'quality':'silver'},
    comment="silver_brach_convenience_score"
)
def silver_branch_convenience_score():
    df=dlt.read("bronze_branch_conveniance_score")
    return df

# COMMAND ----------

from pyspark.sql.functions import sum
import dlt

@dlt.table(
    comment="Gold level: branch-wise overall convenience score"
)
def gold_branch_convenience_score():
    df = dlt.read("silver_branch_convenience_score")

    df_final = df.groupBy("Branch_ID") \
                 .agg(sum("convience_score").alias("overall_score"))

    return df_final


# COMMAND ----------

@dlt.table(
     comment="Location_Wise_Sales"
 )
def gold_location_wise_sales():
     df=dlt.read("customer_sales")
     final_df=df.groupBy("Location").agg(sum("Final_Price").alias("Total_Price"))
     return final_df

# COMMAND ----------

@dlt.table(
     comment="Branch_Wise_Revenue_Generated"
)
def gold_branch_wise_revenue():
     df=dlt.read("customer_sales")
     final_df=df.groupBy("Branch_ID").agg(sum("Final_Price").alias("Total_Revenue"))
     return final_df

# COMMAND ----------

@dlt.table(
     comment="Tags_Wise_Revenue_Generated"
)
def gold_tags_wise_revenue():
     df=dlt.read("customer_sales")
     final_df=df.groupBy("Branch_ID","Model","Tags").agg(sum("Final_Price").alias("Total_Revenue"))
     return final_df

# COMMAND ----------

@dlt.table(
    comment="Service_Branch_revenue"
)
def gold_service_branch_revenue():
    df=dlt.read("customer_service_center")
    final_df=df.groupBy("Branch_ID").agg(sum("Total_Price").alias("Total_Revenue"))
    return final_df

# COMMAND ----------

@dlt.table(
    comment="Branch_Wise_Customers"
)
def gold_Branch_Wise_Customers():
    df=dlt.read("customer_service_center")
    final_df=df.groupBy("Branch_ID").agg(count("customer_id").alias("Total_Customers"))
    return final_df

# COMMAND ----------

@dlt.table(
    comment="Location_wise_Customers"
)
def gold_Location_wise_Customers():
    df=dlt.read("customer_service_center")
    final_df=df.groupBy("Location").agg(count("customer_id").alias("Total_Customers"))
    return final_df

# COMMAND ----------

@dlt.table(
    comment="Branch_wise_Vehicles"
)
def gold_Branch_wise_Vehicles():
    df=dlt.read("customer_service_center")
    final_df=df.groupBy("Branch_ID","Model").agg(count("Model").alias("total_visits"))
    return final_df