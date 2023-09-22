
###################  IMPORTS

from datetime import timedelta,datetime,date
from pathlib import Path
import json
import requests
import psycopg2

import pandas as pd
import os

import smtplib
from email import message

from airflow.exceptions import AirflowSkipException
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials



dag_path = os.getcwd()     #path original.. home en Docker



###################  QUERIES

create_table_query = '''
        CREATE TABLE IF NOT EXISTS spotifysongs (
			noname	        	int,
            track_id        	varchar,
            artists	        	varchar,
			album_name        	varchar,
            track_name        	varchar,
            popularity        	float,
            duration_ms        	float,
            explicit        	bool,
            danceability        float,
            energy        		float,
            track_key        	int,
            loudness        	float,
            track_mode        	float,
            speechiness        	float,
            acousticness        float,
            instrumentalness    float,
            liveness        	float,
            valence        		float,
            tempo        		float,
           	time_signature      int,
           	tack_genre			varchar
            
        );    
    '''

insert_query = '''
            INSERT INTO spotifysongs (noname,track_id,artists,album_name,track_name,popularity,duration_ms,explicit,danceability,energy,track_key,loudness,track_mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature,tack_genre)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        '''


###################  

today = date.today()

tolerated_missing_percentage = 0.001

local_spotify = pd.read_csv(dag_path+'/raw_data/'+"spotify_data.csv",sep=',')

world50_playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"

redshift_conn = {
    'host': Variable.get('redshift_url'),
    'username': Variable.get('redshift_user'),
    'database': Variable.get('redshift_database'),
    'port': Variable.get('redshift_port'),                 
    'pwd': Variable.get('redshift_pwd')
}


default_args = {
    'owner': 'R_Lopez',
    'start_date': datetime(2023,9,20),
    'retries':1,
    'retry_delay': timedelta(seconds=5),
    'catchup': False,
}




###################  FUNCION DE MAILS

def sendMail(sender,password,receiver,subject,body):
    try:
        mailingService = smtplib.SMTP('smtp.gmail.com',587)
        mailingService.starttls()
        mailingService.login(sender,password)
        message='Subject: {}\n\n{}'.format(subject,body)
        mailingService.sendmail(sender,receiver,message)
        print('Exito al enviar el mail')
    except Exception as exception:
        print(exception)
        print('Fallo al enviar el mail')


###################  SPOTIFY

def openSpotifyConnection():
    client_credentials_manager = SpotifyClientCredentials(client_id=Variable.get('spotify_id'), client_secret=Variable.get('spotify_secret'))
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    return sp

def getSongsInPlaylist(playlist_link):
    
    sp = openSpotifyConnection()
    
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    df_test=[]
    try:
        for track in sp.playlist_tracks(playlist_URI)["items"]:
        
            track_uri = track["track"]["id"]
            a = sp.audio_features(track_uri)[0]
            a['track_name'] = track["track"]["name"]
            a['album_name'] = track["track"]["album"]["name"]
            a['explicit'] = track["track"]["explicit"]
            a['popularity']=track["track"]["popularity"]
            a['artists']=track['track']['artists'][0]['name']

            df_test.append(a)
    except Exception as e:
        print("Unable to get top 50 songs")
        print(e)
       
        
    df = pd.DataFrame(df_test)
    return df


###################  CHECKEO Y ENVIO DE MAILS

def checkMissinglData(exec_date,dataframe):

    print(f"Adquiriendo data para la fecha: {exec_date}")
    
    
    dataframe_to_check = dataframe
    missing = dataframe_to_check.isna().sum()   
    most_missing_fields =(missing[missing[:]>0]).round(3)

    missing_percentage = (most_missing_fields.sum()/dataframe_to_check.size).round(5)

    print(exec_date)


    if(missing_percentage >= tolerated_missing_percentage):   
        
        mail_body = f'''
        Percentage threshold for the process: {tolerated_missing_percentage}
        Percentage of missing values tolerated:  {(most_missing_fields.sum()/dataframe_to_check.size).round(5)}
        Amount of total missing values: {(tolerated_missing_percentage*dataframe_to_check.size).round(5)} of {dataframe_to_check.size}
        Fields with most missing values: {most_missing_fields.to_json()}
        '''
        sendMail(Variable.get('mail_sender'),Variable.get('mail_pass'),Variable.get('mail_receiver'),f'Amount of missing values for {exec_date} above threshold',mail_body)
        
    else:
        mail_body = f'''
        Everything went good for {exec_date}
        '''
        sendMail(Variable.get('mail_sender'),Variable.get('mail_pass'),Variable.get('mail_receiver'),f'Revision of {exec_date} process',mail_body)
        


###################  CONEXION BASE DE DATOS

def conexion_redshift(exec_date):

    print(f"Conectandose a la BD en la fecha: {exec_date}") 

    try:
        conn = psycopg2.connect(
            host=redshift_conn["host"],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port=redshift_conn["port"])
        # Crear un cursor
        
        print(conn)
        print("Connected to Redshift successfully!")
        return conn

    except Exception as e:

        print("Unable to connect to Redshift.")
        print(e)


###################  CREA TABLA

def createTable():
    # Crear la tabla en  si no existe
    try:
        conn = conexion_redshift("{{ ds }} {{ execution_date.hour }}")
        cursor = conn.cursor()
        print(conn)
        print("Connected to Redshift successfully!")
        cursor.execute(create_table_query)
        conn.commit()
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    print("trying to create")
    


###################  CARGA DE DATOS EN TABLA

def uploadData(dataframe_to_upload):
    # Carga data en la tabla

    try:
        # Crear un cursor
        conn = conexion_redshift("{{ ds }} {{ execution_date.hour }}")
        cursor = conn.cursor()
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
   
    dataframe = dataframe_to_upload

    for _, row in dataframe.iterrows():
        
        
        cursor.execute(insert_query, tuple(row))
        conn.commit()
        print("one up, 23234 to go")




###################  FUNCIONES PARA DATOS LOCALES
def checkMissingDataLocal():
    checkMissinglData(today,local_spotify)

def uploadLocalData():
    uploadData(local_spotify)


###################  FUNCIONES PARA DATOS API

playlist = getSongsInPlaylist(world50_playlist_link) # Get top 50 songs and convert to data frame

def checkMissingDataPlaylistTop50():
    checkMissinglData(today,playlist)

def uploadTop50Data():
    uploadData(playlist)





###################  DAGS

with DAG(
    
    dag_id="Spotify_data",
    default_args= default_args,
    description="Agrega data de spotify. Local y de api",
    start_date=datetime(2023,9,3,2),
    schedule_interval='@daily' 
    
    ) as dag:
    
    task_0 = PythonOperator(
    task_id='create_spotify_table',
    python_callable=createTable,
    op_args=[]
    
    )

    task_1 = PythonOperator(
    task_id='check_missing_local',
    python_callable=checkMissingDataLocal,
    op_args=[]
    
    )
    

    task_2 = PythonOperator(
    task_id='upload_local_data',
    python_callable=uploadLocalData,
    op_args=[]
    
    )

    task_3 = PythonOperator(
    task_id='check_top_50_songs',
    python_callable=checkMissingDataPlaylistTop50,
    op_args=[]
    
    )

    task_4 = PythonOperator(
    task_id='upload_top_50_songs',
    python_callable=uploadTop50Data,
    op_args=[]
    
    )

    task_0.set_downstream(task_2)
    task_1.set_downstream(task_2)

    task_0.set_downstream(task_3)
    task_3.set_downstream(task_4)
   