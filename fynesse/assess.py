from .config import *

from . import access

import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

def remove_price_outliers(conn, remove_below=0, remove_above=None):
  cur = conn.cursor()
  above_condition = (" OR price > " + str(remove_above)) if remove_above is not None else ""
  cur.execute("DELETE FROM pp_data WHERE price < " + str(remove_below) + above_condition)
  cur.commit()

def sales_date_maxmin(conn):
  cur = conn.cursor()
  cur.execute("SELECT MIN(date_of_transfer), MAX(date_of_transfer) FROM pp_data")

  res = cur.fetchall()[0]
  return res[0], res[1]

def remove_missing_postcodes(conn):
  cur = conn.cursor()
  cur.execute("DELETE FROM pp_data WHERE postcode=''")
  cur.commit()

#returns min lat, max lat, min lon, max lon
def longlat_maxmin(conn)
    cur = conn.cursor()
    cur.execute("SELECT MIN(lattitude), MAX(lattitude), MIN(longitude), MAX(longitude) FROM postcode_data")
    minlat, maxlat, minlon, maxlon = cur.fetchall()[0]
    return minlat, maxlat, minlon, maxlon

def get_pois(lat, lon, box_size, tags):
  north = lat + box_size/2
  south = lat - box_size/2
  west = lon - box_size/2
  east = lon + box_size/2

  pois = ox.geometries_from_bbox(north, south, east, west, tags)
  return pois

#cache results of num_of_pois_by_tag_type as it can take a long time to run
num_pois_cache = {}

def num_of_pois_by_tag_type(lat, lon, box_size, tag):
  if ((lat,lon), tag) not in num_pois_cache:
    num_pois_cache[((lat, lon), tag)] = len(get_pois(lat, lon, box_size, {tag: True}))
  
  return num_pois_cache[((lat, lon), tag)]

def num_of_pois_by_tag_types(lat, lon, box_size, tags):
  return len(get_pois(lat, lon, box_size, tags))

#scale to between 0 and 1
def scaled_lats(conn, lats):
  cur = conn.cursor()
  cur.execute("SELECT MIN(lattitude), MAX(lattitude) FROM postcode_data")
  minlat, maxlat = cur.fetchall()[0]

  scaled_lats = [(lat - float(minlat)) / float(maxlat - minlat) for lat in lats]

  return scaled_lats

#scale to between 0 and 1
def scaled_lons(conn, lons):
  cur = conn.cursor()
  cur.execute("SELECT MIN(longitude), MAX(longitude) FROM postcode_data")
  minlon, maxlon = cur.fetchall()[0]

  scaled_lons = [(lon - float(minlon)) / float(maxlon - minlon) for lon in lons]

  return scaled_lons



def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError

def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError

def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError

def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError
