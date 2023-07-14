import hq_data_manager as hdm


# Definition of path [modify]
shp_file_path =      '../Misc/contour_bassin.shp'
metadata_file_path = '../Misc/station_metadata.yaml'
database_name =      '../hq_open_data_Romaine.db'
archive_path =       '../Archive/'


### Example 1: Step by step creation of the database
# Pull data
json, response = hdm.data.pull()
# Select stations
shp = hdm.metadata.load_shapefile(shp_file_path)
station_ids = hdm.metadata.select_station(json, shp)
# Extract metadata
stations_metadata = hdm.metadata.get(json, station_ids)
# Create database
hdm.database.create(database_name)
conn, cursor = hdm.database.connect(database_name)
for station_id in stations_metadata.keys():
    hdm.database.insert_station(conn, cursor, station_id, stations_metadata[station_id]['name'])
hdm.database.disconnect(conn,cursor)


### Example 2: One step database creation (similar to example 1)
hdm.database.initialize(shp_file_path, metadata_file_path, database_name)


### Example 3: Automatic collection, archiving and insertion into the database.
# Can send an email if it continuously fail retrieving data (edit utils.send_warning_mail )
hdm.utils.scheduler(metadata_file_path, archive_path, database_name, send_mail=True)


### Example 4: Read archive
my_archive = hdm.data.read_archive(archive_path+'my_file.pkl.gz')