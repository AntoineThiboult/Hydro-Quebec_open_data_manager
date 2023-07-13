# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:11:57 2023

@author: ANTHI182
"""

import os
import json
import requests
import datetime
import pickle
import gzip


def json_loader(json_file_path):
    """
    Load a json file and return its content

    Parameters
    ----------
    json_file_path : String
        Path to the json file that contains data.
        Example: './Donnees_VUE_STATIONS_ET_TARAGES.json'
    Returns
    -------
    json : json dictionary
        Hydro-Québec data as json dictionnary
    """

    # Open JSON file
    with open(json_file_path, encoding='utf-8') as fh:
        data = json.load(fh)

    return data


def pull():
    """
    Pull the Hydro-Québec open data from their server


    Returns
    -------
    json : json dictionary
        Hydro-Québec data as json dictionnary
    Request status : int
        Requests status code (200 = success, 404 = fail because of unaccessible
        target, 999 for any other issue)
    """

    url = 'https://www.hydroquebec.com/data/documents-donnees/donnees-ouvertes/json/Donnees_VUE_STATIONS_ET_TARAGES.json'
    try:
        response = requests.get(url)
        response.encoding = response.apparent_encoding
        return response.json(), response.status_code

    except Exception as e:
        print(f'Error: {e}')
        return None, 999


def archive(archive_path, data):
    """
    Compress and save as a tar file

    Parameters
    ----------
    filename : String
        Relative or absolute path to a directory where data is stored
    data : Any type of object that can be tar
        Data to compress and save

    Returns
    -------
    None.
    """

    # Insert extraction date in filename
    extraction_date = datetime.datetime.now().strftime("%y-%m-%d--%H-%M")
    filename = os.path.join(
        archive_path, f'hq_open_data_{extraction_date}.pkl.gz')

    with gzip.open(filename, 'wb') as file:
        pickle.dump(data, file)


def read_archive(filename):
    """
    Decompress and read a file pkl.gz file

    Parameters
    ----------
    filename : String
        Relative or absolute path of the file where data is stored

    Returns
    -------
    data: saved item
    """

    with gzip.open(filename, 'rb') as file:
        data = pickle.load(file)
    return data