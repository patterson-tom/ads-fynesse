# ADS Fynesse Library 

## Access

Gaining access to the data, including overcoming availability challenges (data is distributed across architectures, called from an obscure API, written in log books) as well as legal rights (for example intellectual property rights) and individual privacy rights (such as those provided by the GDPR).

`create_connection(user, password, host, database, port=3306)` - Create a database connection to a MariaDB database

`create_pp_data_table(conn)` - Create the pp_data table, defining the schema and indexes

`load_pp_data(conn)` - Download pp_data data and place it into the table, can take a long time due to amount of data

`create_postcode_data_table(conn)` - Create the postcode_data table, defining the schema and indexes

`load_postcode_data(conn)` - Download postcode_data data and place it into the table

`create_prices_coordinates_data_table(conn)` - Create the prices_coordinates_data table, defining the schema and indexes

`join_on_postcode_in_range(conn, lat, lon, box_size, start_date, end_date)` - Join the pp_data and postcode_data records which satisfy the given spatial and temporal constraints, storing the result in prices_coordinates_data

`data(conn)` - Creates all tables and loads all data

## Assess

Understanding what is in the data. Is it what it's purported to be, how are missing values encoded, what are the outliers, what does each variable represent and how is it encoded.

`remove_price_outliers(conn, remove_below=0, remove_above=None)` - delete records from pp_data where the price is outside of the given filters. Useful for removing anomalous entries e.g. houses selling for £1

`sales_date_maxmin(conn)` - returns the earliest and latest sale in pp_data

`remove_missing_postcodes(conn)` - deletes records in pp_data which have missing postcode info.

`longlat_maxmin(conn)` - returns the minimum and maximum long/lat values in postcode_data

`remove_anomalous_lat_values(conn)` - removes the weird latitude values from postcode_data which are on the equator instead of where they claim to be (in Scotland).

`get_pois(lat, lon, box_size, tags)` - returns the pois from open street map within the bounding box and with the given tags

`num_of_pois_by_tag_type(lat, lon, box_size, tag)` - returns the number of pois from open street map within the bounding box and with the given tag

`num_of_pois_by_tag_types(lat, lon, box_size, tags)` - returns the number of pois from open street map within the bounding box and with the given tags

`scaled_lats(conn, lats)` - returns the given lat values scaled to between 0 and 1 where 0 is the least latitude value in postcode_data, and 1 is the highest

`scaled_lons(conn, lons)` - returns the given lon values scaled to between 0 and 1 where 0 is the least longitude value in postcode_data, and 1 is the highest

`sales_over_time(conn, use_precomputed_result=True)` - returns the number of sales in each year in pp_data. By default, uses the saved values as the SQL query used takes a long time to run, but can be recalculated if requested.

`postcode_district_sales(conn, use_precomputed_values=True)` - returns the number of sales in each postcode district in pp_data. By default, uses the saved values as the SQL query used takes a long time to run, but can be recalculated if requested.

## Address

The final aspect of the process is to *address* the question. We'll spend the least time on this aspect here, because it's the one that is most widely formally taught and the one that most researchers are familiar with. In statistics, this might involve some confirmatory data analysis. In machine learning it may involve designing a predictive model. In many domains it will involve figuring out how best to visualise the data to present it to those who need to make the decisions. That could involve a dashboard, a plot or even summarisation in an Excel spreadsheet.

`get_num_pois_sample(conn, sample_size=35)` - returns a dataframe containing a sample of sales from pp_data, augmented with long/lat info and the number of different pois from open street map within a 5km bounding box 

`get_date_range(conn, date)` - calculate the desired date range in months in either direction based on `assess.sales_over_time`. Formula used is `round((peak_year_sales - sales_in_year / (peak_year_sales / min_year_sales) * 3) + 3`. The aim is to ensure that the number of items in the dataset remains constant independent of the date the prediction is called with.

`predict_price(conn, latitude, longitude, date, property_type)` - actually make a prediction using the methodology described in the notebook. Selects an appropriate bounding box, builds a training and test set, trains a guassian model with parameters based on local POIs and the property type, then makes prediction and assesses quality of model using test set, and returns prediction, warning of lower quality models appropriately.
