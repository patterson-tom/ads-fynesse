# This file contains code for suporting addressing questions in the data

from . import access
from . import assess

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

import math
from shapely.geometry import Point

import warnings

"""Address a particular question that arises from the data"""

def get_num_pois_sample(conn, sample_size=35):
    sql_query = pd.read_sql_query('SELECT sample.price, postcode_data.lattitude, postcode_data.longitude FROM (SELECT * FROM pp_data LIMIT '+str(sample_size)+') sample INNER JOIN postcode_data ON (sample.postcode = postcode_data.postcode)', conn)
    df = pd.DataFrame(sql_query)

    poi_types = ["amenity", "emergency", "healthcare", "highway", "leisure", "man_made", "military", "power", "public_transport", "railway", "sport", "tourism"]

    for t in poi_types:
      num = []
      for _, row in df.iterrows():
        num.append(assess.num_of_pois_by_tag_type(row['lattitude'], row['longitude'], 0.05, t))
      df['num_'+t] = num

    return df

def get_date_range(conn, date):
  sales_data = assess.sales_over_time(conn, use_precomputed_result=True)
  year = date.split("-")[0]
  return round((max(sales_data.values()) - sales_data[year]) / (max(sales_data.values()) - min(sales_data.values())) * 3) + 3

def make_prediction(results, property_type, num_amenities, num_emergencies, num_healthcares):
  x_pred = [num_amenities, num_emergencies, num_healthcares, property_type=="F", property_type=="S", property_type=="D", property_type=="T", property_type=="O"]
  return results.get_prediction(x_pred).predicted_mean[0]

def predict_price(conn, latitude, longitude, date, property_type):
  date_range = get_date_range(conn, date)

  datetime_obj = datetime.strptime(date, "%Y-%m-%d")
  earliest_obj = datetime_obj - relativedelta(months=date_range)
  latest_obj = datetime_obj + relativedelta(months=date_range)

  earliest = datetime.strftime(earliest_obj, "%Y-%m-%d")
  latest = datetime.strftime(latest_obj, "%Y-%m-%d")

  #load up table with relevant data
  df = pd.DataFrame(columns=["property_type"])
  box_size = 0
  requirement = 30
  while len(df) < requirement or property_type not in df.property_type.unique():
    print("Attempting to construct training set...")
    box_size += max(0.01, box_size)
    requirement = max(10, requirement - 10)
    access.join_on_postcode_in_range(conn, latitude, longitude, box_size, earliest, latest)

    #load table into dataframe
    sql_query = pd.read_sql_query('SELECT * FROM prices_coordinates_data', conn)
    df = pd.DataFrame(sql_query)
    print(len(df), "sales found on this attempt")

  print("Using this training set")

  warnings.simplefilter(action='ignore', category=UserWarning)

  #nearby amenities from osm
  amenities = assess.get_pois(latitude, longitude, box_size+0.03, {"amenity": True})
  warnings.simplefilter(action='ignore', category=UserWarning)
  num_amenities = []
  for _, row in df.iterrows():
    num_amenities.append(len(amenities[amenities.distance(Point(row['longitude'], row['lattitude'])) < 0.02]))

  df['num_amenities'] = num_amenities

  #nearby emergencies from osm
  emergencies = assess.get_pois(latitude, longitude, box_size+0.03, {"emergency": True})
  warnings.simplefilter(action='ignore', category=UserWarning)
  num_emergencies = []
  for _, row in df.iterrows():
    num_emergencies.append(len(emergencies[emergencies.distance(Point(row['longitude'], row['lattitude'])) < 0.02]))

  df['num_emergencies'] = num_emergencies

  #nearby healthcares from osm
  healthcares = assess.get_pois(latitude, longitude, box_size+0.03, {"healthcare": True})
  warnings.simplefilter(action='ignore', category=UserWarning)
  num_healthcares = []
  for _, row in df.iterrows():
    num_healthcares.append(len(healthcares[healthcares.distance(Point(row['longitude'], row['lattitude'])) < 0.02]))

  df['num_healthcares'] = num_healthcares

  #split df into train and test
  np.random.seed(42)
  df_test = pd.DataFrame()
  try_again = True
  while try_again:
    train_mask = np.random.rand(len(df)) < 0.8
    df_test = df[~train_mask]
    df_train = df[train_mask]
    try_again = property_type not in df_train.property_type.unique()

  design = np.concatenate((df_train.num_amenities.values.reshape(-1,1), df_train.num_emergencies.values.reshape(-1,1), df_train.num_healthcares.values.reshape(-1,1), (df_train.property_type=="F").values.reshape(-1,1), (df_train.property_type=="S").values.reshape(-1,1), (df_train.property_type=="D").values.reshape(-1,1), (df_train.property_type=="T").values.reshape(-1,1), (df_train.property_type=="O").values.reshape(-1,1)),axis=1)
  m_linear = sm.OLS(df_train.price,design)
  results = m_linear.fit()

  #validate on testing set
  avg_err = 0
  for _, row in df_test.iterrows():
    pred =  make_prediction(results, row['property_type'], row['num_amenities'], row['num_emergencies'], row['num_healthcares'])
    if pred == 0:
      continue
    err = abs(pred - row['price'])
    avg_err += err / pred

  avg_err /= len(df_test)

  num_amenities = len(amenities[amenities.distance(Point(longitude, latitude)) < 0.02])
  num_emergencies = len(emergencies[emergencies.distance(Point(longitude, latitude)) < 0.02])
  num_healthcares = len(healthcares[healthcares.distance(Point(longitude, latitude)) < 0.02])

  pred = make_prediction(results, property_type, num_amenities, num_emergencies, num_healthcares)
  if avg_err > 0.3:
    print("Warning: the prediction may have poor quality, having an average error of", 100 * avg_err, "% on the test set")

  return pred
