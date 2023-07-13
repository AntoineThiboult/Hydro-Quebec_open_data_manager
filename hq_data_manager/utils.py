# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:11:57 2023

@author: ANTHI182
"""

import time
import schedule
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from . import data
from . import database

def collect_archive_insert_measurements(metadata_file_path, archive_path, database_name, send_mail=False):
    """
    Collect, archive and insert measurements in the database. Send an email if
    there are too many unsuccessful connection attempts. This is a
    hight level function for automation.

    Parameters
    ----------
    metadata_file_path : String
        Path to a yaml metadatafile. Example: './watershed.yaml'
    archive_path : String
        Path to a directory where data will be archived in a compress pickle
    database_name : String
        Path (relative or absolute) including the database name
    send_mail : Boolean (default False)
        Boolean to indicate if a warning email should be sent in the case of
        repeated data collection failure.

    Returns
    -------
    None.
    """

    max_connect = 15*20 # (maximum number of connection attempts before sending warning)
    time_between_connect = 5*60 # (in seconds)

    # Download data to create sample
    json, response = data.pull()

    # Try to connect to the Hydro-Québec server.
    conn_counter = 0
    while (response != 200) & (conn_counter < max_connect):
        conn_counter += 1
        print('Could not retrieve data from Hydro-Quebec serveur. ',
              'Error code: {response}')
        json, response = data.pull()
        time.sleep(time_between_connect)

    # Send warning by email if max number of attempts reached.
    if conn_counter == max_connect:
        print('Maximum number of connection attempt reached.')
        send_warning_mail()
        return

    data.archive(archive_path, json)
    database.insert_measurements(metadata_file_path, database_name)


def scheduler(metadata_file_path, archive_path, database_name):
    """
    Program automatic collection, archiving and insertion of measurements

    Parameters
    ----------
    metadata_file_path : String
        Path to a yaml metadatafile. Example: './watershed.yaml'
    archive_path : String
        Path to a directory where data will be archived in a compress pickle
    database_name : String
        Path (relative or absolute) including the database name

    Returns
    -------
    None.
    """

    schedule.every().day.at("12:40").do( collect_archive_insert_measurements,
                                        metadata_file_path, archive_path, database_name)
    while True:
        schedule.run_pending()
        time.sleep(1)


def send_warning_mail():
    """
    Send warning email.

    Returns
    -------
    None.
    """

    fromaddr = "your_gmail@gmail.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['Subject'] = "Hydro-Quebec open database -- Error "
    body = "There is an issue with the daily data collection. Please check the "\
        "connection."
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "your_password")

    toaddrList = ["recipient@gmail.com"]
    for iDest in toaddrList:
        msg['To'] = iDest
        text = msg.as_string()
        server.sendmail(fromaddr, iDest, text)
    server.quit()

    print("Warning email sent!\n")