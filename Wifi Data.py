# Databricks notebook source
# MAGIC %md #Creating The Environment 

# COMMAND ----------

import numpy as np
import pandas as pd
import pyspark.pandas as ps


# COMMAND ----------

pip install databricks-cli

# COMMAND ----------

# MAGIC %sh pip install networkx

# COMMAND ----------

import networkx

# COMMAND ----------

# MAGIC %md # Reading In Files 

# COMMAND ----------

df_02 = pd.read_parquet("dbfs:/FileStore/wifi/wifi-cleansed/2022/10/01/00/cleansedwifitraps_20221002040917.parquet")


# COMMAND ----------

df_03 = pd.read_parquet('/dbfs/FileStore/wifi/wifi-cleansed/2022/10/02/00/cleansedwifitraps_20221003040847.parquet')

# COMMAND ----------

mapping = pd.read_csv('/dbfs/FileStore/wifi/station mapping/2020/03/02/17/stationextendedpaths_20200302174537.csv')


# COMMAND ----------

mapping.display()

# COMMAND ----------

metadata = pd.read_csv('/dbfs/FileStore/wifi/station mapping/2019/10/25/21/stationmetadata_20191025212131.csv') #showing station name, group and Prefix

# COMMAND ----------

metadata.sort_values('StationName').display()#

Elizabeth_line = df_03.loc[(df_03.AccessPointPrefix == 'heath4', 'heath1')] 
                            #'heat5','bondst','canary','farrin', 'livpst', 'padcir', 'paddin' 'wtchpl','ealbdy','stratf','totcrt')]

# COMMAND ----------

meta = pd.read_csv('/dbfs/FileStore/wifi/station mapping/2020/06/23/07/nonlulinks_20200623072758.csv')

meta.display()

# COMMAND ----------

path_sequence = pd.read_csv('/dbfs/FileStore/wifi/station mapping/2019/12/09/16/nodes_20191209161231.csv/part_00000_tid_5965681666348671588_1eee92ac_1587_4cb0_a1c5_7bf0d8692d6a_50809_1_c000.csv')

# COMMAND ----------

path_sequence.display()

# Is ticket hall lobby in this dataset 

# COMMAND ----------

wap_location = path_sequence[['ZoneDescription','AtAp']].copy()
wap_location.display()

# COMMAND ----------


df_03['CaptureTimeLocal'] = df_03['CaptureTimeLocal'].astype('datetime64[ns]')
df_03.sort_values("CaptureTime").display()


# COMMAND ----------

oxfcir = df_03.loc[(df_03.AccessPointPrefix == "oxfcir")]
oxfcir.display()

# COMMAND ----------

oxfcir_wap_location =  pd.merge(oxfcir,wap_location, left_on='AccessPointName', right_on='AtAp', how='left')

# COMMAND ----------

oxfcir_wap_location.display()

# COMMAND ----------

df_03[["ClientMacAddress"]].count()

# COMMAND ----------

df_03[['ClientMacAddress']].max()

# COMMAND ----------

g_address = df_03.loc[(df_03.ClientMacAddress == "Gary3qZD5s9ZcrWqPz7mFwN2Q+ySNB+LiaUa6u0tBLU=")]

g_address.display()

#filtered_df = df.where("id > 1")

# COMMAND ----------

Elizabeth_line = df.loc[(df.AccessPointPrefix == 'knight')].count()


# COMMAND ----------

g_address.sort_values(['CaptureTimeLocal']).display()

# COMMAND ----------

x = df_03['ClientMacAddress'].unique().sum
print(x)


# COMMAND ----------

bond_street = df_03.loc[(df_03.AccessPointPrefix == "bondst") & (df_03.TravelDay == '2022-10-01')]
bond_street.display()

# COMMAND ----------

bond_street[['AccessPointPrefix']].count()


# COMMAND ----------

bond_street.sort_values(['ProcessedTimeLocal']).display()

# COMMAND ----------

df.sort_values(["TravelDay"]).display()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.info()

# COMMAND ----------

df['CaptureTimeLocal'] = df['CaptureTimeLocal'].astype('datetime64[ns]')

# COMMAND ----------

df['TravelDay'] = df['TravelDay'].astype('datetime64[ns]')

# COMMAND ----------

df.display()

# COMMAND ----------

df2 = ps.read_parquet ('dbfs:/FileStore/wifi/wifi-curated/2022/10/02/00/stationrouting_20221003061545.parquet/part_00001_tid_1051565670487661852_4a32311b_ad1b_4a40_9777_a5489897d124_4251_1_c000_snappy.parquet')

# COMMAND ----------

df2.display()

# COMMAND ----------

df2['ClientMacAddress'].max()


# COMMAND ----------

mac_address = df2.loc[(df2.ClientMacAddress == "zzneBJuF2Sq44WPKsQESjuvo1U0RADwIb04ppfek3OY=")]
#g_address = df.loc[(df.ClientMacAddress == "Gary3qZD5s9ZcrWqPz7mFwN2Q+ySNB+LiaUa6u0tBLU=")]

