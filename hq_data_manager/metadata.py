# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:11:57 2023

@author: ANTHI182
"""

import yaml
import geopandas as gpd


class Station:
    """
    Creates station object to store and organize Hydro-Québec station metadata
    """

    def __init__(self, name, identifier):
        self.name = name
        self.identifier = identifier
        self.measurement_types = {}

    def add_measurement_type(self, type_id, unit, timestep, measurement_description):
        self.measurement_types[type_id] = {
            "unit": unit,
            "timestep": timestep,
            "measurement_description": measurement_description}


def load(metadata_file_path):
    """
    Load the yaml file that contains station metadata

    Parameters
    ----------
    metadata_file_path : String
        Path to a yaml metadatafile. Example: './watershed.yaml'

    Returns
    -------
    metadata :
    """

    with open(metadata_file_path, 'r', encoding='utf-8') as file:
        metadata = yaml.safe_load(file)

    return metadata


def get(json_data, station_identifiers):
    """
    Extract Hydro-Québec station metadata from a json file.

    Parameters
    ----------
    json_file : String
        Path to the json file that contains data.
        Example: './Donnees_VUE_STATIONS_ET_TARAGES.json'
    station_identifiers : List of strings
        List of Hydro-Québec station identifiers to be processed

    Returns
    -------
    station_dict : Dictionary
        Dictionary that contains Hydro-Québec metadata
    """

    station_dict = dict()

    for s in json_data['Station']:

        if s['identifiant'] in station_identifiers:
            # Create a station object containing measurement information and ID
            station = Station(s['nom'], s['identifiant'])
            for m in s['Composition']:
                station.add_measurement_type(m['type_point_donnee'],
                                             m['nom_unite_mesure'],
                                             m['pas_temps'],
                                             m['type_mesure'])
            # Store metadata in dictionary
            station_dict[station.identifier] = {
                "name": station.name,
                "identifier": station.identifier,
                "measurement_types": station.measurement_types
            }

    return station_dict

def dump(station_dict, yaml_file):
    """
    Write metadata to a yaml file.

    Parameters
    ----------
    station_dict : Dictionary
        Dictionary that contains Hydro-Québec metadata
    yaml_file : String
        Path to the yaml file to be writen. Example: './Data/metadata.yaml'

    Returns
    -------
    None.
    """

    with open(yaml_file, 'w', encoding='utf-8') as file:
        yaml.dump(station_dict, file, allow_unicode=True)


def select_station(json_data, shapefile):
    """
    Select open data Hydro-Québec stations that are contained within a user
    specified area. Returns a list that contains stations identifier located
    within the area.

    Parameters
    ----------
    json_file : json file
        Path to the json file that contains data.
        Example: './Donnees_VUE_STATIONS_ET_TARAGES.json'
    shapefile : Geodataframe
        Shapefile used to select stations

    Returns
    -------
    stations : List of strings
        List of Hydro-Québec station identifiers of the stations located within
        the specified area
    """

    stations = list()

    for s in json_data['Station']:
        # Create geopandas point and check if within Romaine watershed
        pnt = gpd.points_from_xy(x=[float(s['xcoord'])],
                                 y=[float(s['ycoord'])],
                                 z=None)

        if pnt.within(shapefile.geometry)[0]:
            stations.append(s['identifiant'])

    return stations


def load_shapefile(shp_file_path):
    """
    Load a shapefile and return it as a Geodataframe

    Parameters
    ----------
    shp_file_path : String
        Path to a shapefile. Example: './watershed.shp'

    Returns
    -------
    shp : Geodataframe
        Shapefile
    """

    # Load shapefile for station selection
    shp = gpd.read_file(shp_file_path)

    return shp