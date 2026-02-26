# Databricks notebook source
df = spark.read.csv("workspace.us_flights.us_flights_2023", header=True, inferSchema=True)

# COMMAND ----------

# Carregando Tabela com DataFrame
df = spark.read.table("workspace.us_flights.us_flights_2023")
display(df)

# COMMAND ----------

# Conferindo o Schema
df.printSchema()

# COMMAND ----------

# CRIANDO DELAY TOTAL
# Tratando Dados Nulos, referenciar colunas e criar valores literais (fixos)
from pyspark.sql.functions import coalesce, col, lit
df = df.withColumn(
    "Total_Delay",
    coalesce(col("Dep_Delay"), lit(0)) +
    coalesce(col("Arr_Delay"), lit(0))
)

# COMMAND ----------

# Criando Status do Voo
from pyspark.sql.functions import when
df = df.withColumn(
    "Flight_Status",
    when(col("Total_Delay") <= 0, "On Time")
    .when(col("Total_Delay") <= 30, "Minor Delay")
    .otherwise("Major Delay")
)


# COMMAND ----------

# Criando Período do dia
df = df.withColumn(
    "Day_Period",
    when(col("DepTime_label") == "Morning", "Morning")
    .when(col("DepTime_label") == "Afternoon", "Afternoon")
    .when(col("DepTime_label") == "Evening", "Evenving")
    .otherwise("Night")
)

# COMMAND ----------

# Criando Tabela FATO (fact_flights)
# Armazena eventos mensuráveis do negócio

fact_flights = df.select(
    "FlightDate",
    "Airline",
    "Tail_Number",
    "Dep_Airport",
    "Arr_Airport",
    "Total_Delay",
    "Flight_Status",
    "Day_Period"
)

fact_flights.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("us_flights.fact_flights")

# COMMAND ----------

df = spark.read.table("workspace.us_flights.fact_flights")
display(df)

# COMMAND ----------

# Criando Dimensões 
# Dimensão Data
from pyspark.sql.functions import year, month, dayofmonth, dayofweek

dim_date = df.select("FlightDate").distinct() \
    .withColumn("Year", year("FlightDate")) \
    .withColumn("Month", month("FlightDate")) \
    .withColumn("Day", dayofmonth("FlightDate")) \
    .withColumn("DayOfWeek", dayofweek("FlightDate"))

dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("us_flights.dim_date")

# COMMAND ----------

df = spark.read.table("workspace.us_flights.dim_date")
display(df)

# COMMAND ----------

# DBTITLE 1,Dimensão Airline
# Dimensão Airline
fact_df = spark.read.table("workspace.us_flights.fact_flights")
dim_airline = fact_df.select("Airline").distinct()

dim_airline.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("us_flights.dim_airline")

# COMMAND ----------

# DBTITLE 1,Cell 12
# Dimensão Airport
from pyspark.sql.functions import col
fact_df = spark.read.table("workspace.us_flights.fact_flights")
dim_airport = fact_df.select(
    col("Dep_Airport").alias("Airport_Code")
).distinct()

dim_airport.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("us_flights.dim_airport")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criando View para Dashboard (Power BI)
# MAGIC CREATE OR REPLACE VIEW us_flights.vw_airline_dashboard AS 
# MAGIC SELECT
# MAGIC     f.FlightDate,
# MAGIC     d.Year,
# MAGIC     d.Month,
# MAGIC     f.Airline,
# MAGIC     f.Dep_Airport,
# MAGIC     f.Arr_Airport,
# MAGIC     f.Total_Delay,
# MAGIC     f.Flight_Status,
# MAGIC     f.Day_Period
# MAGIC FROM us_flights.fact_flights f
# MAGIC JOIN us_flights.dim_date d
# MAGIC ON f.FlightDate = d.FlightDate;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC -- Análise SQL
# MAGIC -- Atraso Médio por Aeroporto
# MAGIC SELECT Dep_Airport,
# MAGIC   AVG(Total_Delay) AS Avg_Delay
# MAGIC FROM us_flights.fact_flights
# MAGIC GROUP BY Dep_Airport
# MAGIC ORDER BY Avg_Delay DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ranking de Companhias Mais Pontuais
# MAGIC SELECT Airline,
# MAGIC        AVG(Total_Delay) AS Avg_Delay
# MAGIC FROM us_flights.fact_flights
# MAGIC GROUP BY Airline
# MAGIC ORDER BY Avg_Delay ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tendência Mensal
# MAGIC SELECT year,
# MAGIC        Month,
# MAGIC        COUNT (*) AS Total_flight,
# MAGIC        AVG(Total_Delay) AS Avg_Delay
# MAGIC FROM us_flights.vw_airline_dashboard
# MAGIC GROUP BY Year, Month
# MAGIC ORDER BY Year, Month;

# COMMAND ----------

# Particionar por Ano
fact_flights.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("FlightDate") \
    .saveAsTable("us_flights.fact_flights_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Otimizar Delta
# MAGIC OPTIMIZE us_flights.fact_flights
# MAGIC ZORDER BY (Airline, Dep_Airport);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Camada GOLD
# MAGIC -- Tabela Agregada Mensal
# MAGIC CREATE OR REPLACE TABLE us_flights.gold_airline_performance
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     YEAR(FlightDate) AS Year,
# MAGIC     MONTH(FlightDate) AS Month,
# MAGIC     Airline,
# MAGIC     Dep_Airport,
# MAGIC     Arr_Airport,
# MAGIC     COUNT(*) AS Total_Flights,
# MAGIC     AVG(Total_Delay) AS Avg_Delay,
# MAGIC     SUM(CASE WHEN Flight_Status = 'On Time' THEN 1 ELSE 0 END) 
# MAGIC         / COUNT(*) AS OnTime_Rate
# MAGIC FROM us_flights.fact_flights
# MAGIC GROUP BY 
# MAGIC   YEAR(FlightDate), 
# MAGIC   MONTH(FlightDate), 
# MAGIC   Airline, 
# MAGIC   Dep_Airport, 
# MAGIC   Arr_Airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Executiva
# MAGIC CREATE OR REPLACE VIEW us_flights.vw_powerbi_dashboard AS
# MAGIC SELECT
# MAGIC     Year,
# MAGIC     Month,
# MAGIC     Airline,
# MAGIC     Dep_Airport,
# MAGIC     Arr_Airport,
# MAGIC     Total_Flights,
# MAGIC     ROUND(Avg_Delay, 2) AS Avg_Delay,
# MAGIC     ROUND(OnTime_Rate * 100, 2) AS OnTime_Percentage
# MAGIC FROM us_flights.gold_airline_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE us_flights.gold_airline_performance AS
# MAGIC SELECT
# MAGIC     YEAR(FlightDate) AS Year,
# MAGIC     MONTH(FlightDate) AS Month,
# MAGIC     Airline,
# MAGIC     COUNT(*) AS Total_Flights,
# MAGIC     SUM(CASE WHEN Flight_Status = 'On Time' THEN 1 ELSE 0 END) AS OnTime_Count,
# MAGIC     AVG(Total_Delay) AS Avg_Delay
# MAGIC FROM us_flights.fact_flights
# MAGIC GROUP BY
# MAGIC     YEAR(FlightDate),
# MAGIC     MONTH(FlightDate),
# MAGIC     Airline;