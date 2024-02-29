import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Variable du .env
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_NAME_STAGING')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def insert_infos_especes_staging(csv_path):
    try:
        # Chargement des données depuis le CSV
        df_infos_espece = pd.read_csv(csv_path, delimiter=';')

        # Connexion à la base de données
        staging_conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_staging,
            user=user,
            password=password)
        
        stg_cursor = staging_conn.cursor()

        df_infos_espece.fillna('', inplace=True)

        for index, df_line in df_infos_espece.iterrows():
            query = "INSERT INTO stg_infos_espece (espece, description, nom_latin, famille, region, habitat, fun_fact, taille) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            stg_cursor.execute(query, (df_line['Espèce'], df_line['Description'], df_line['Nom latin'], df_line['Famille'], df_line['Région'], df_line['Habitat'], df_line['Fun fact'], df_line['Taille']))
        
        # Confirmation des transactions
        staging_conn.commit()
        print("Les informations du CSV ont bien été inséré dans la base de données de staging.")

    except Error as e:
        print(f"Erreur lors de la récupération des données depuis la base de données: {e}")
    finally:
        if staging_conn.is_connected():
            stg_cursor.close()
            staging_conn.close()