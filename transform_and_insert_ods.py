from PIL import Image
import mysql.connector
from mysql.connector import Error
import base64
import re
import pandas as pd
from PIL import Image
from io import BytesIO
import io
import os
import warnings

# Ignorer les avertissements FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)

# Variable du .env
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_NAME_STAGING')
db_ods = os.getenv('DB_NAME_ODS')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def resize_image(base64_string, size=(800, 600)):
    image_data = BytesIO(base64_string)
    img = Image.open(image_data)
    if img.mode == 'RGBA':
        img = img.convert('RGB')
    resized_img = img.resize(size)
    buffered = io.BytesIO()
    resized_img.save(buffered, format="JPEG")
    resized_image_data = buffered.getvalue()
    return resized_image_data 

def convertir_en_cm(taille_str):
    nombres = re.findall(r'\d+', taille_str)
    if not nombres:
        return 0
    nombres = [int(n) for n in nombres]
    taille_cm = nombres[0]
    if 'cm' in taille_str:
        return taille_cm
    elif 'm' in taille_str:
        return taille_cm * 100
    else:
        return taille_cm  


def transform_and_insert_ods():
    try:
        staging_conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_staging,
            user=user,
            password=password)
        
        ods_conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_ods,
            user=user,
            password=password)
        
        if ods_conn.is_connected():
            stg_image_cursor = staging_conn.cursor(dictionary=True)
            stg_infos_especes_cursor = staging_conn.cursor(dictionary=True)
            ods_image_cursor = ods_conn.cursor()
            ods_infos_especes_cursor = ods_conn.cursor()

# --------------------------------------------- TABLE IMAGE ---------------------------------------------

            stg_image_cursor.execute("SELECT id, image_blob, created_at, coordinates, espece, user FROM stg_image")
            data_stg_image = stg_image_cursor.fetchall()

            if data_stg_image != []:
                for row in data_stg_image:
                    if row['user'] is not None:
                        user_id = row['user'].split('-')[1]
                    else:
                        user_id = None

                    # Redimensionne l'image
                    img_resized = resize_image(row['image_blob'])

                    # Insère les données dans l'ODS
                    insert_query = "INSERT INTO ods_image (image_blob, created_at, coordinates, espece, user) VALUES (%s, %s, %s, %s, %s)"
                    ods_image_cursor.execute(insert_query, (img_resized, row['created_at'], row['coordinates'], row['espece'], user_id))
                    ods_conn.commit()

                # Si tout s'est bien passé, vider la table stg_image
                stg_image_cursor.execute("DELETE FROM stg_image")
                staging_conn.commit()

                print("Données 'image' du STAGING transformées, insérées dans l'ODS et supprimées du STAGING !")

            else:
                print("Aucune données 'image' du STAGING à transformer !")

# --------------------------------------------- TABLE INFOS ESPECE ---------------------------------------------

            stg_infos_especes_cursor.execute("SELECT id, espece, description, nom_latin, famille, region, habitat, fun_fact, taille FROM stg_infos_espece")
            data_stg_infos_especes = stg_infos_especes_cursor.fetchall()
            
            if data_stg_infos_especes != []:
                data_stg_infos_especes = pd.DataFrame(data_stg_infos_especes)

                # Liste des colonnes textuelles à traiter
                colonnes_textuelles = ['description', 'habitat', 'fun_fact']

                # Remplace les valeurs manquantes dans les colonnes textuelles par 'Information non disponible'
                data_stg_infos_especes[colonnes_textuelles] = data_stg_infos_especes[colonnes_textuelles].applymap(lambda x: 'Information non disponible' if pd.isna(x) or x == '' else x)

                # Remplace les valeurs manquantes dans la colonne 'Taille' par '0 cm'
                data_stg_infos_especes['taille'] = data_stg_infos_especes['taille'].fillna('0 cm')

                # Applique une fonction pour convertir les tailles en centimètres
                data_stg_infos_especes['taille_cm'] = data_stg_infos_especes['taille'].apply(convertir_en_cm)

                # Supprime la colonne 'Taille' d'origine
                data_stg_infos_especes.drop(columns=['taille'], inplace=True)

                for index, df_line in data_stg_infos_especes.iterrows():
                    query = "INSERT INTO ods_infos_espece (espece, description, nom_latin, famille, region, habitat, fun_fact, taille_cm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    ods_infos_especes_cursor.execute(query, (df_line['espece'], df_line['description'], df_line['nom_latin'], df_line['famille'], df_line['region'], df_line['habitat'], df_line['fun_fact'], df_line['taille_cm']))

                ods_conn.commit()

                # Si tout s'est bien passé, vider la table stg_image
                stg_infos_especes_cursor.execute("DELETE FROM stg_infos_espece")
                staging_conn.commit()

                print("Données 'infos_espece' du STAGING transformées, insérées dans l'ODS et supprimées du STAGING !")

            else:
                print("Aucune données 'infos_espece' du STAGING à transformer !")

    except Error as e:
        print(f"Erreur lors de la récupération des données depuis la base de données: {e}")
    finally:
        if staging_conn.is_connected():
            stg_image_cursor.close()
            stg_infos_especes_cursor.close()
            staging_conn.close()
        if ods_conn.is_connected():
            ods_image_cursor.close()
            ods_infos_especes_cursor.close()
            ods_conn.close()
