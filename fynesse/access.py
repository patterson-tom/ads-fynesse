from .config import *

import pymysql

import pandas as pd # used to download and save csvs
import urllib.request
import zipfile

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database,
                               autocommit=True
                               )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn

def create_pp_data_table(conn):
    """
    Create pp_data table
    """
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `pp_data`")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS `pp_data` (
                  `id` tinytext COLLATE utf8_bin NOT NULL,
                  `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                  `price` int(10) unsigned NOT NULL,
                  `date_of_transfer` date NOT NULL,
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                  `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `street` tinytext COLLATE utf8_bin NOT NULL,
                  `locality` tinytext COLLATE utf8_bin NOT NULL,
                  `town_city` tinytext COLLATE utf8_bin NOT NULL,
                  `district` tinytext COLLATE utf8_bin NOT NULL,
                  `county` tinytext COLLATE utf8_bin NOT NULL,
                  `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                  `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 """)

    #also add primary key and index certain columns
    cur.execute("""ALTER TABLE `pp_data`
                    ADD PRIMARY KEY (`db_id`)""")
    cur.execute("""ALTER TABLE `pp_data` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1""")
    cur.execute("""CREATE INDEX `pp.postcode` USING HASH
                      ON `pp_data`
                        (postcode)""")
    cur.execute("""CREATE INDEX `pp.date` USING HASH
                      ON `pp_data` 
                        (date_of_transfer);""")
    
    conn.commit()

def create_postcode_data_table(conn):
    """
    Create postcode_data table
    """
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `postcode_data`")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS `postcode_data` (
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `status` enum('live','terminated') NOT NULL,
                  `usertype` enum('small', 'large') NOT NULL,
                  `easting` int unsigned,
                  `northing` int unsigned,
                  `positional_quality_indicator` int NOT NULL,
                  `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
                  `lattitude` decimal(11,8) NOT NULL,
                  `longitude` decimal(10,8) NOT NULL,
                  `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
                  `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
                  `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
                  `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
                  `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
                  `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
                  `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
                  `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin""")

    #also add primary key and index certain columns
    cur.execute("""ALTER TABLE `postcode_data`
                    ADD PRIMARY KEY (`db_id`)""")
    cur.execute("""ALTER TABLE `postcode_data` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1""")
    cur.execute("""CREATE INDEX `po.postcode` USING HASH
                      ON `postcode_data`
                        (postcode)""")
    cur.execute("""CREATE INDEX `po.lattitude` USING HASH
                      ON `postcode_data`
                        (lattitude)""")
    cur.execute("""CREATE INDEX `po.longitude` USING HASH
                      ON `postcode_data`
                        (longitude);""")
    
    conn.commit()

def create_prices_coordinates_data_table(conn):
    """
    Create prices_coordinates_data table
    """
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS `prices_coordinates_data`")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
                  `price` int(10) unsigned NOT NULL,
                  `date_of_transfer` date NOT NULL,
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                  `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `locality` tinytext COLLATE utf8_bin NOT NULL,
                  `town_city` tinytext COLLATE utf8_bin NOT NULL,
                  `district` tinytext COLLATE utf8_bin NOT NULL,
                  `county` tinytext COLLATE utf8_bin NOT NULL,
                  `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
                  `lattitude` decimal(11,8) NOT NULL,
                  `longitude` decimal(10,8) NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 """)

    #also add primary key
    cur.execute("""ALTER TABLE `prices_coordinates_data`
                    ADD PRIMARY KEY (`db_id`);""")
    
    conn.commit()

def upload_csv_file_to_pp_data_table(conn, filepath):
    """
    Upload the contents of filepath to 'pp_data' table
    """
    cur = conn.cursor()
    cur.execute("""LOAD DATA LOCAL INFILE %s INTO TABLE pp_data 
                   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
                   LINES STARTING BY '' TERMINATED BY '\n'""", (filepath,))
    
    conn.commit()

def download_and_save_csv(url, filepath):
  df = pd.read_csv(url)
  df.to_csv(filepath)

def download_and_unzip_file(url, direc):
  f, _ = urllib.request.urlretrieve(url)
  zipped_f = zipfile.ZipFile(f, 'r')
  zipped_f.extractall(direc)

def upload_csv_file_to_postcode_data_table(conn, filepath):
    """
    Upload the contents of filepath to 'postcode_data' table
    """
    cur = conn.cursor()
    cur.execute("""LOAD DATA LOCAL INFILE %s INTO TABLE postcode_data 
                   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
                   LINES STARTING BY '' TERMINATED BY '\n'""", (filepath,))

    conn.commit()

def load_pp_data(conn):
    for year in range(1995, 2022):
      url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-" + str(year) + ".csv"
      local_filepath = str(year) + "_price_paid.csv"
      download_and_save_csv(url, local_filepath)
      upload_csv_file_to_pp_data_table(conn, local_filepath)
      print(year,"done")

def load_postcode_data(conn):
    download_and_unzip_file("https://www.getthedata.com/downloads/open_postcode_geo.csv.zip", "./postcode_data")
    upload_csv_file_to_postcode_data_table(conn, "postcode_data/open_postcode_geo.csv")

def join_on_postcode_in_range(conn, lat, lon, box_size, start_date, end_date):
  cur = conn.cursor()

  #clear joined table
  cur.execute("TRUNCATE TABLE prices_coordinates_data;")

  #join
  cur.execute("""
              INSERT INTO prices_coordinates_data
              SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, po.country, po.lattitude, po.longitude, pp.db_id FROM
              (SELECT * FROM pp_data WHERE date_of_transfer > %s AND date_of_transfer < %s) pp 
              INNER JOIN
              (SELECT * FROM postcode_data WHERE lattitude > %s AND lattitude < %s AND longitude > %s AND longitude < %s) po
              ON (pp.postcode = po.postcode)
              """, (start_date, end_date, lat-box_size/2, lat+box_size/2, lon-box_size/2, lon+box_size/2))

#can use this to setup everything at once
def data(conn):
    create_pp_data_table(conn)
    load_pp_data(conn)

    create_postcode_data_table(conn)
    load_postcode_data(conn)

    create_prices_coordinates_data_table(conn)

