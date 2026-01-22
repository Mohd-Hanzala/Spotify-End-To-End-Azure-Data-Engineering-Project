# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###**DimUser**

# COMMAND ----------

df = spark.read.format("parquet")\
          .load("abfss://bronze@azuredatalakeproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **AUTOLOADER**

# COMMAND ----------

# DBTITLE 1,Cell 5
df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load('abfss://bronze@azuredatalakeproject.dfs.core.windows.net/DimUser')
            

# COMMAND ----------

# for s in spark.streams.active:
 # s.stop()

# COMMAND ----------

display(df_user,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/checkpoit")




# COMMAND ----------

df_user=df_user.withColumn("user_name",upper(col("user_name")))


# COMMAND ----------

display(df_user,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/checkpoit1")

# COMMAND ----------

#path_copy kara h
#  /Workspace/Users/mohdhanzala542_gmail.com#ext#@mohdhanzala542gmail337.onmicrosoft.com/spotify_dab/utils/transformation.py
from spotify_dab.utils.transformations import *

# because of need to import the file, our root directory(spotify_dab) of this project is added in the system path 

# COMMAND ----------

#Get Current Working Directory (Main abhi kis folder me khada hu?)

import os 

print(os.getcwd())

# COMMAND ----------

''' os.getcwd()
👉 Current working directory batata hai
(“Abhi Python kis folder me run ho raha hai”)

. (dot)
👉 Current folder

.. (double dot)
👉 Parent folder (ek level upar)

os.path.join(path, '..')
👉 Path ko safely jodta hai aur parent directory ka path deta hai

print(os.path.join(os.getcwd(), '..'))
👉 Current folder se ek level upar ka path print karta hai '''

import os 

print(os.path.join(os.getcwd(),'..'))

# COMMAND ----------

# 2 folder upar (usually project root)

import os

project_path = os.path.join(os.getcwd(),'..','..')


#Python ka control panel (paths, exit, args)

import sys 

sys.path.append(project_path)

from utils.transformations import reusable



# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumn(df_user,'_rescued_data')
df_user = df_user.dropDuplicates(['user_id'])




# COMMAND ----------

display(df_user,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/checkpoint11")

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_catalog.silver.DimUser")

     ###.start("path","abfss://s.......DimUser/data")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###**DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load('abfss://bronze@azuredatalakeproject.dfs.core.windows.net/DimArtist')

# COMMAND ----------

df_artist_obj = reusable()

df_artist = df_artist_obj.dropColumn(df_artist, ['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ###**DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load('abfss://bronze@azuredatalakeproject.dfs.core.windows.net/DimTrack')

# COMMAND ----------

display(df_track,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimTrack/checkpoint_display/display1")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

#I want to add a durationflag based on the artist_id column
# like if esle elif
df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
.when(col('duration_sec')<300,"medium")\
.otherwise("high"))

## Replace hyphens (-) with spaces in the track_name column for better readability
df_track = df_track.withColumn(
    "track_name",
    regexp_replace(col("track_name"), "-", " ")
)



# COMMAND ----------


display(df_track,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimTrack/checkpoint_display/display3")

# COMMAND ----------

df_track_obj = reusable()

df_track = df_track_obj.dropColumn(df_track, ['_rescued_data'])
df_track = df_track.dropDuplicates(['artist_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ##**DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimDate/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load('abfss://bronze@azuredatalakeproject.dfs.core.windows.net/DimDate')

# COMMAND ----------

df_track_obj = reusable()

# Drop unwanted column using reusable method
df_date = df_track_obj.dropColumn(df_date, ['_rescued_data'])


# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azuredatalakeproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ###**FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/FactStream/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load('abfss://bronze@azuredatalakeproject.dfs.core.windows.net/FactStream')

# COMMAND ----------

display(df_fact,checkpointLocation="abfss://silver@azuredatalakeproject.dfs.core.windows.net/FactStream/checkpoint_display/display1")

# COMMAND ----------

df_fact = reusable().dropColumn(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@azuredatalakeproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@azuredatalakeproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_catalog.silver.FactStream")