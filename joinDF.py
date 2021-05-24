from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    print("Session Started")

spark = SparkSession.builder.appName("joinDF").master("local[*]") \
    .getOrCreate()

print("Session Created")

df1 = spark.read.csv("G:/streamingfiles/user.csv", inferSchema=True, header=True)

# print(df1.show())

df2 = spark.read.csv("G:/streamingfiles/account.csv", inferSchema=True, header=True)

# print(df2.show())

joindf = df1.join(df2, "User_ID")
print(joindf.show())
# finaldf = joindf.groupBy("User_ID").agg(f.collect_set("Account_ID")).f.sum("Balance")
# finaldf = joindf.groupBy("User_ID").agg(f.collect_list("Account_ID")) \
#     .agg(f.collect_list("Balance"))
finaldf = joindf.groupBy("User_ID").agg(f.sum("Balance").alias("Total Balance"), f.collect_list("Account_ID"),
                                        f.avg("Balance").alias("Average Balance"))

print(finaldf.show())
