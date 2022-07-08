# Databricks notebook source
from pyspark.sql.functions import col, lower, when, regexp_replace
import plotly.express as px
import pandas as pd
import seaborn as sns
from sklearn.manifold import TSNE


# COMMAND ----------

df_master = spark.table('esp_sandbox.esp_master')
display(df_master)

# COMMAND ----------

df_failure = spark.table('esp_sandbox.esp_failure')

# COMMAND ----------

# MAGIC %md Many formats for ESP, needs to clean it up

# COMMAND ----------

df_failure.filter(lower(col('LiftMethod')).contains('esp')).select('LiftMethod').distinct().show()

# COMMAND ----------

df_esp = df_failure.withColumn('LiftMethod',  when(lower(col("LiftMethod")).contains('esp'), 'ESP').otherwise(col('LiftMethod'))).filter(col('LiftMethod')=='ESP')

# COMMAND ----------

df_esp.select('LiftMethod').distinct().show()

# COMMAND ----------

df_esp.columns

# COMMAND ----------

df_esp_sub= df_esp.select('ProblemIDWell','ProblemType', 'cause', 'CAUSEDETAIL', 'LiftMethod',
                          'ProblemStart', 'FailureEndDate', 'FieldEstimateCost', 'dttmpullcalc', 'dttmruncalc', 'durstarttoruncalc', 'ResolvedDate', 
                          'FailureDepth', 'wellname', 'fieldname', 'basin', 'area', 'county', 'Country')

# COMMAND ----------

display(df_esp_sub)

# COMMAND ----------

df_agg = df_esp_sub.filter(col('FailureEndDate')>='2018-01-01').groupBy('ProblemType').count().dropna()

# COMMAND ----------

display(df_agg)

# COMMAND ----------

df_agg_pd = df_agg.toPandas()
df_agg_pd.loc[df_agg_pd['count']<df_agg_pd['count'].quantile(0.5), 'ProblemType'] = 'Other'

# COMMAND ----------

fig = px.pie(df_agg_pd, values='count', names='ProblemType', title='Failure Component')
fig.show()

# COMMAND ----------

df_agg_cause = df_esp_sub.filter(col('FailureEndDate')>='2018-01-01').groupBy('cause').count().dropna()
display(df_agg_cause)

# COMMAND ----------

df_agg_cause_pd = df_agg_cause.toPandas()
df_agg_cause_pd.loc[df_agg_cause_pd['count']<df_agg_cause_pd['count'].quantile(0.5), 'cause'] = 'Other'
fig = px.pie(df_agg_cause_pd, values='count', names='cause', title='Failure Type')
fig.show()

# COMMAND ----------

df_esp_from_2018 = df_esp_sub.filter(col('FailureEndDate')>='2018-01-01')
display(df_esp_from_2018)

# COMMAND ----------

df_esp_from_2018_pd = df_esp_from_2018.toPandas()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

cols = ['FieldEstimateCost']
df = df_esp_from_2018_pd
Q1 = df[cols].quantile(0.25)
Q3 = df[cols].quantile(0.75)
IQR = Q3 - Q1
df = df[~((df[cols] < (Q1 - 1.5 * IQR)) |(df[cols] > (Q3 + 1.5 * IQR))).any(axis=1)]
g =sns.histplot(data=df, x='FieldEstimateCost', bins=30, kde=False, hue='area', multiple='stack')
#g.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()


# COMMAND ----------

# MAGIC %md #Clustering

# COMMAND ----------

df_esp_sub_pd = df_esp_sub.toPandas()

# COMMAND ----------

df_esp_sub_pd.head()

# COMMAND ----------

columns = ['ProblemIDWell', 'ProblemType', 'cause', 'area', 'FieldEstimateCost']
df_esp_sub_pd = df_esp_sub_pd[columns]
df_esp_sub_pd = df_esp_sub_pd.set_index('ProblemIDWell')

# COMMAND ----------

from sklearn.preprocessing import OrdinalEncoder
enc = OrdinalEncoder()
cat_columns = ['ProblemType', 'cause', 'area']
df_esp_sub_pd[cat_columns] = enc.fit_transform(df_esp_sub_pd[cat_columns])

# COMMAND ----------

df_esp_sub_pd[cat_columns]=df_esp_sub_pd[cat_columns].astype(int)

# COMMAND ----------

df_esp_sub_pd.head()

# COMMAND ----------

df_esp_sub_pd = df_esp_sub_pd.dropna()

# COMMAND ----------

# MAGIC %md ##PCA

# COMMAND ----------

from sklearn.decomposition import PCA
pca = PCA(n_components=2)
components = pca.fit_transform(df_esp_sub_pd)

labels = {
    str(i): f"PC {i+1} ({var:.1f}%)"
    for i, var in enumerate(pca.explained_variance_ratio_ * 100)
}

fig = px.scatter_matrix(
    components,
    labels=labels,
    dimensions=range(2),
    color=df_esp_sub_pd["ProblemType"]
)
fig.update_traces(diagonal_visible=False)
fig.show()

# COMMAND ----------

tsne = TSNE(n_components=2, verbose=1, random_state=123)
projections = tsne.fit_transform(df_esp_sub_pd) 
fig = px.scatter(
    projections, x=0, y=1, color = df_esp_sub_pd.cause, labels={'color':'cause'})
fig.update_traces(marker_size=8)
fig.show()

# COMMAND ----------

tsne = TSNE(n_components=3, verbose=1, random_state=123)
projections = tsne.fit_transform(df_esp_sub_pd) 
fig = px.scatter_3d(
    projections, x=0, y=1, z=2, color = df_esp_sub_pd.cause, labels={'color':'cause'})
fig.update_traces(marker_size=8)
fig.show()

# COMMAND ----------

from sklearn.cluster import KMeans
import numpy as np

kmeans = KMeans(n_clusters=10, random_state=0).fit(df_esp_sub_pd)
clusters = kmeans.labels_

#kmeans.predict([[0, 0], [12, 3]])

#kmeans.cluster_centers_

# COMMAND ----------

df_esp_sub_pd.insert(0, "Cluster", clusters,allow_duplicates =False)

# COMMAND ----------

df_esp_sub_pd.head()

# COMMAND ----------

fig = px.scatter(df_esp_sub_pd, x='ProblemType', y='area', color = df_esp_sub_pd.Cluster, labels={'color':'Cluster'})
fig.update_traces(marker_size=8)
fig.show()

# COMMAND ----------

fig = px.scatter(df_esp_sub_pd, x='ProblemType', y='FieldEstimateCost', color = df_esp_sub_pd.Cluster, labels={'color':'Cluster'})
fig.update_traces(marker_size=8)
fig.show()

# COMMAND ----------

fig = px.scatter(df_esp_sub_pd, x='ProblemType', y='FieldEstimateCost', color = df_esp_sub_pd.Cluster, labels={'color':'Cluster'})
fig.update_traces(marker_size=8)
fig.show()

# COMMAND ----------

fig = px.scatter_3d(df_esp_sub_pd, x='ProblemType', y='FieldEstimateCost', z='area',
              color='Cluster')
fig.show()

# COMMAND ----------

from sklearn.cluster import DBSCAN
import numpy as np
X = np.array([[1, 2], [2, 2], [2, 3],
              [8, 7], [8, 8], [25, 80]])
clustering = DBSCAN(eps=3, min_samples=2).fit(df_esp_sub_pd.drop('Cluster', axis=1))
clustering.labels_
