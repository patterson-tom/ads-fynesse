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

"""Address a particular question that arises from the data"""

def make_prediction(results, property_type, num_amenities):
  x_pred = [num_amenities, property_type=="F", property_type=="S", property_type=="D", property_type=="T", property_type=="O"]
  return results.get_prediction(x_pred).predicted_mean[0]

def predict_price(latitude, longitude, date, property_type):
  box_size = 0.1
  datetime_obj = datetime.strptime(date, "%Y-%m-%d")
  earliest_obj = datetime_obj - relativedelta(months=12)
  latest_obj = datetime_obj + relativedelta(months=12)
  earliest = datetime.strftime(earliest_obj, "%Y-%m-%d")
  latest = datetime.strftime(latest_obj, "%Y-%m-%d")

  #load up table with relevant data
  access.join_on_postcode_in_range(conn, latitude, longitude, box_size, earliest, latest)

  #load table into dataframe
  conn = access.create_connection(user=credentials["username"], 
                         password=credentials["password"],
             host=database_details["url"],
             database="property_prices")
  sql_query = pd.read_sql_query('SELECT * FROM prices_coordinates_data', conn)
  df = pd.DataFrame(sql_query)

  print(len(df), " sales considered")

  #nearby amenities from osm
  amenities = assess.get_pois(latitude, longitude, box_size+0.03, {"amenity": True})
  amenities = amenities.set_crs(epsg=4326)

  #calc features
  num_amenities = []
  for _, row in df.iterrows():
    num_amenities.append(len(amenities[amenities.distance(Point(row['longitude'], row['lattitude'])) < 0.02]))

  df['num_amenities'] = num_amenities

  #split df into train and test
  np.random.seed(42)
  train_mask = np.random.rand(len(df)) < 0.8
  df_test = df[~train_mask]
  df = df[train_mask]

  design = np.concatenate((df.num_amenities.values.reshape(-1,1) / 100, (df.property_type=="F").values.reshape(-1,1), (df.property_type=="S").values.reshape(-1,1), (df.property_type=="D").values.reshape(-1,1), (df.property_type=="T").values.reshape(-1,1), (df.property_type=="O").values.reshape(-1,1)),axis=1)
  m_linear = sm.OLS(df.price,design)
  results = m_linear.fit()

  #validate on testing set
  avg_err = 0
  for _, row in df_test.iterrows():
    pred =  make_prediction(results, row['property_type'], row['num_amenities'])
    err = abs(pred - row['price'])
    avg_err += err / pred

  avg_err /= len(df_test)

  num_amenities = assess.num_of_pois_by_tag_type(latitude, longitude, 0.02, "amentity")
  pred = make_prediction(results, property_type, num_amenities)
  if avg_err > 0.2:
    print("Warning: the prediction may have poor quality, having an average error of", 100 * avg_err, "% on the test set")

  return pred
