from pyspark.ml.clustering import KMeans, KMeansModel
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import os

cur_model=None

def anomaly_detection(df_kmeans):
  kmeans = KMeans(k=2, seed=0).setFeaturesCol('features')
  model = kmeans.fit(df_kmeans)
  centers = model.clusterCenters()
  current_path = os.getcwd()
  model.write().overwrite().save(current_path + "/kmean_model")
  transform = model.transform(df_kmeans).select('prediction')
  labels = transform.collect()
  points = df_kmeans.collect()
  points_array = np.asarray(points)
  labels_array = np.asarray(labels)
  plot_anomalies(points_array, labels_array)
  return centers


def find_anomalies(points):
  global cur_model
  if cur_model is None:
    model_path = os.getcwd() + "/kmean_model"
    cur_model = KMeansModel.load(model_path)

  labels = cur_model.transform(points).select('prediction')
  points_array = np.asarray(points.collect())
  labels_array = np.asarray(labels.collect())
  results =[]
  for item, label in zip(points_array, labels_array):
    temp = []
    temp.append(item[0][0])
    temp.append(item[0][1])
    temp.append(item[0][2])
    temp.append(label[0])
    results.append(temp)
    print(results)
  return results


def plot_anomalies(points, labels):
  fig = plt.figure()
  ax = fig.add_subplot(111, projection='3d')
  data = zip(points, labels)
  for item, label in zip(points, labels):
    if label == 0:
      ax.scatter(item[0][0], item[0][1], item[0][2], c='r', marker='o')
    else:
      ax.scatter(item[0][0], item[0][1], item[0][2], c='b', marker='s')

  ax.set_xlabel('changeOverTime')
  ax.set_ylabel('changePercent')
  ax.set_zlabel('volume')
  plt.show()
