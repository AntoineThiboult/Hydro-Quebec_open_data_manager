# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:11:57 2023

@author: ANTHI182
"""

import sqlite3
from . import data
from . import metadata

def connect(database_name):
    """
    Connect to the local database

    Parameters
    ----------
    database_name : String
        Path (relative or absolute) including the database name

    Returns
    -------
    conn : Object
        Connection object con represents the connection to the on-disk database.
    cursor : Object
        Database cursor.
    """

    try:
        # Connect to the SQLite database (creates a new database if it doesn't exist)
        conn = sqlite3.connect(database_name)
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f'Operation aborted: {e}')


def disconnect(conn, cursor):
    """
    Disconnect from the local database

    Parameters
    ----------
    conn : Object
        Connection object con represents the connection to the on-disk database.
    cursor : Object
        Database cursor.

    Returns
    -------
    None.
    """

    try:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Operation aborted: {e}')


def create(database_name):
    """
    Create a local database with a structure that is suitable to host
    Hydro-Québec open data

    Parameters
    ----------
    database_name : String
        Path (relative or absolute) including the database name

    Returns
    -------
    None.
    """

    conn, cursor = connect(database_name)

    # Create the Stations table
    create_stations_table_query = '''
        CREATE TABLE IF NOT EXISTS Stations (
            ID TEXT PRIMARY KEY,
            Name TEXT
        )
    '''
    cursor.execute(create_stations_table_query)

    # Create the Measurements table
    create_measurements_table_query = '''
        CREATE TABLE IF NOT EXISTS Measurements (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            StationID TEXT,
            MeasurementType TEXT,
            Value REAL,
            Unit TEXT,
            Timestamp TEXT,
            FOREIGN KEY (StationID) REFERENCES Stations(ID)
        )
    '''
    cursor.execute(create_measurements_table_query)

    # Create a UNIQUE index on the Measurements table for the combination
    # of StationID, MeasurementType, and Timestamp
    create_index_query = "CREATE UNIQUE INDEX IF NOT EXISTS unique_measurement "\
        "ON Measurements (StationID, MeasurementType, Unit, Timestamp)"
    cursor.execute(create_index_query)

    disconnect(conn,cursor)


def insert_station(conn, cursor, station_id, station_name):
    """
    Insert a new station in the database. If the station already exist, the
    command is ignored.

    Parameters
    ----------
    conn : Object
        Connection object con represents the connection to the on-disk database.
    cursor : Object
        Database cursor.
    station_id : String
        Unique identifier for the station
    station_name : String
        Station name

    Returns
    -------
    None.
    """

    # Insert a station into the Stations table
    station_data = (station_id, station_name)
    insert_station_query = "INSERT OR IGNORE INTO Stations (ID, Name) VALUES (?, ?)"
    cursor.execute(insert_station_query, station_data)
    conn.commit()


def insert_measurement(measurement, conn, cursor):
    """
    Insert a single measurement to the database.

    Parameters
    ----------
    measurement : Tuple
        Tuple that contains the data. Must have the format:
            (StationID, MeasurementType, Value, Unit, Timestamp)
    conn : Object
        Connection object con represents the connection to the on-disk database.
    cursor : Object
        Database cursor.

    Returns
    -------
    None.
    """

    # Insert measurements into the Measurements table
    insert_measurement_query = "INSERT OR REPLACE INTO Measurements (StationID, "\
        "MeasurementType, Value, Unit, Timestamp) VALUES (?, ?, ?, ?, ?)"
    cursor.executemany(insert_measurement_query, measurement)
    conn.commit()


def format_data(measurement, var_name, unit, station_id):
    """
    Convert the Hydro-Québec json format to a clean tuple suitable for sqlite.

    Parameters
    ----------
    measurement : json / dictionary
        Data to be formated
    var_name : Sring
        Name of the variable
    unit : String
        Variable unit
    station_id : String
        Unique identifier for the station

    Returns
    -------
    formated_data : Tuple
        Tuple that contains the data with the format:
            (StationID, MeasurementType, Value, Unit, Timestamp)
    """

    # Dictionnary that translates Hydro-Québec names to more standard names.
    name_translation = {
        'Débit':'streamflow',
        'Humidité relative.2 mètres': 'rel_hum',
        'Niveau': 'level',
        'Précipitation': 'precip_tot',
        'Température Maximum': 'temp_max',
        'Température Minimum': 'temp_min',
        'Épaisseur de neige': 'snow_depth',
        'Équivalent en eau de la neige': 'swe'}

    formated_data = []
    for timestamp, value in measurement.items():
        formated_data.append((station_id, name_translation[var_name], float(value), unit, timestamp))

    return formated_data


def initialize(shp_file_path, metadata_file_path, database_name):
    """
    Initialize the database. Select the stations to be kept from a shapefile,
    extract relevant metadata and save them. Create a database with a suitable
    structure and add the stations to it.

    Parameters
    ----------
    shp_file_path : String
        Path to a shapefile. Example: './watershed.shp'
    metadata_file_path : String
        Path to a yaml metadatafile. Example: './watershed.yaml'
    database_name : String
        Path (relative or absolute) including the database name

    Returns
    -------
    None.
    """

    # Download data to create sample
    json, _ = data.pull()

    # Select stations
    shp = metadata.load_shapefile(shp_file_path)
    station_ids = metadata.select_station(json, shp)

    # Extract metadata
    stations_metadata = metadata.get(json, station_ids)

    # Save metadata for later use
    metadata.dump(stations_metadata, metadata_file_path)

    # Create database
    create(database_name)
    conn, cursor = connect(database_name)

    for station_id in stations_metadata.keys():
        insert_station(conn, cursor, station_id, stations_metadata[station_id]['name'])

    disconnect(conn,cursor)


def insert_measurements(json, metadata_file_path, database_name):
    """
    Loop over the measurements in the json to include them in the database

    Parameters
    ----------
    json : json dictionary
        Hydro-Québec data as json dictionnary
    metadata_file_path : String
        Path to a yaml metadatafile. Example: './watershed.yaml'
    database_name : String
        Path (relative or absolute) including the database name

    Returns
    -------
    None.
    """

    stations_metadata = metadata.load(metadata_file_path)
    conn, cursor = connect(database_name)

    for station in json['Station']:
        # Treat only station that are selected
        if station['identifiant'] in  stations_metadata.keys():
            for var in station['Composition']:
                db_var = format_data(var['Donnees'],
                                                  var['type_point_donnee'],
                                                  var['nom_unite_mesure'],
                                                  station['identifiant'])
                insert_measurement(db_var, conn, cursor)

    disconnect(conn,cursor)