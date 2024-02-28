import mysql.connector
from mysql.connector import Error
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, TimestampType
import base64
import pandas as pd
import os

# Variable du .env
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_NAME_STAGING')
db_ods = os.getenv('DB_NAME_ODS')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def load_and_insert_datawarehouse():
    spark = SparkSession.builder.appName("To Datawarehouse").getOrCreate()
    try:
        ods_conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_ods,
            user=user,
            password=password)
        
        datawarehouse_conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_ods,
            user=user,
            password=password)
        
        if datawarehouse_conn.is_connected():
            ods_image_cursor = ods_conn.cursor(dictionary=True)
            # ods_infos_especes_cursor = ods_conn.cursor(dictionary=True)
            dw_empreinte_cursor = datawarehouse_conn.cursor()
            dw_animal_cursor = datawarehouse_conn.cursor()

# --------------------------------------------- TABLE EMPREINTE ---------------------------------------------

            ods_image_cursor.execute("SELECT id, image_blob, created_at, coordinates, espece, user FROM ods_image")
            data_ods_image = ods_image_cursor.fetchall()

            data_ods_image = pd.DataFrame(data_ods_image)

            print(data_ods_image)


    except Error as e:
        print(f"Erreur lors de la récupération des données depuis la base de données: {e}")
    finally:
        spark.stop()
        if ods_conn.is_connected():
            ods_image_cursor.close()
            # ods_infos_especes_cursor.close()
            dw_empreinte_cursor.close()
            dw_animal_cursor.close()
            ods_conn.close()