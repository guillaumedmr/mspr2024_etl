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
db_ods = os.getenv('DB_NAME_ODS')
db_dw = os.getenv('DB_NAME_DW')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def load_and_insert_datawarehouse():
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
            database=db_dw,
            user=user,
            password=password)
        
        if datawarehouse_conn.is_connected():
            ods_image_cursor = ods_conn.cursor(dictionary=True)
            ods_infos_especes_cursor = ods_conn.cursor(dictionary=True)
            dw_empreinte_cursor = datawarehouse_conn.cursor(dictionary=True)
            dw_animal_cursor = datawarehouse_conn.cursor(dictionary=True)

# --------------------------------------------- TABLE ANIMAL ---------------------------------------------

            # Récupération des données de l'ODS
            ods_infos_especes_cursor.execute("SELECT id, espece, description, nom_latin, famille, region, habitat, fun_fact, taille_cm FROM ods_infos_espece")
            data_ods_infos_espece = ods_infos_especes_cursor.fetchall()
            data_ods_infos_espece = pd.DataFrame(data_ods_infos_espece)

            # Création de l'enregistrement pour l'espèce inconnue
            ligne_inconnue = {'id': -1, 'espece': 'Inconnu', 'description': 'Information non disponible', 'nom_latin': 'Incognito', 'famille': 'Information non disponible', 'region': 'Information non disponible', 'habitat': 'Information non disponible', 'fun_fact': 'Information non disponible', 'taille_cm': 0}

            # Création d'un DataFrame à partir de la nouvelle ligne
            df_nouvelle_ligne = pd.DataFrame([ligne_inconnue])

            # Concaténation du DataFrame existant avec la nouvelle ligne
            data_ods_infos_espece = pd.concat([data_ods_infos_espece, df_nouvelle_ligne], ignore_index=True)

            # Récupération des données du Datawarehouse afin de comparer les données pour l'ajout
            dw_animal_cursor.execute("SELECT id_animal, img_animal, statut_animal, nom_animal, nom_latin_animal, habitat_animal, region_animal, funfact_animal, description_animal, taille_animal FROM animal")
            data_dw_infos_espece = dw_animal_cursor.fetchall()
            data_dw_infos_espece = pd.DataFrame(data_dw_infos_espece)
            data_ods_infos_espece = data_ods_infos_espece.rename(columns={'espece': 'nom_animal'})

            if data_dw_infos_espece.empty:
                pass
            else:
                # Comparaison et résultat des données à rajouter
                data_ods_infos_espece = data_ods_infos_espece[~data_ods_infos_espece['nom_animal'].isin(data_dw_infos_espece['nom_animal'])]

            if data_ods_infos_espece.empty:
                print("Aucune nouvelles données 'infos_espece' dans l'ODS !")
            else:
                for index, row in data_ods_infos_espece.iterrows():
                    query = "INSERT INTO animal (statut_animal, nom_animal, nom_latin_animal, habitat_animal, region_animal, funfact_animal, description_animal, taille_animal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    dw_animal_cursor.execute(query, ("Information non disponible", row["nom_animal"], row["nom_latin"], row["habitat"], row["region"], row["fun_fact"], row["description"], row["taille_cm"]))

                # Confirmation des transactions
                datawarehouse_conn.commit()
                print("Les informations du CSV dans ODS ont bien été inséré dans la base de données de Datawarehouse.")

# --------------------------------------------- TABLE EMPREINTES ---------------------------------------------
            # Récupération des données de l'ODS 
            ods_image_cursor.execute("SELECT id, image_blob, created_at, coordinates, espece, user FROM ods_image")
            data_ods_image = ods_image_cursor.fetchall()
            data_ods_image = pd.DataFrame(data_ods_image)

            # Récupération des données du Datawarehouse afin de récupérer les ID des espèces
            dw_animal_cursor.execute("SELECT id_animal, nom_animal FROM animal")
            data_dw_espece_id = dw_animal_cursor.fetchall()
            data_dw_espece_id = pd.DataFrame(data_dw_espece_id)

            # Remplacement des noms des espèces par leur ID            
            data_ods_image = pd.merge(left=data_ods_image, right=data_dw_espece_id, left_on='espece', right_on='nom_animal', how='left')
            data_ods_image = data_ods_image.drop(columns=['espece', 'nom_animal'])
            data_ods_image['id_animal'] = data_ods_image['id_animal'].fillna(data_dw_espece_id[data_dw_espece_id['nom_animal']=='Inconnu']['id_animal'].iloc[0])

            # Récupération des données du Datawarehouse afin de comparer les données pour l'ajout
            dw_empreinte_cursor.execute("SELECT id_empreinte, coordonnee_empreinte, img_empreinte, date_empreinte, id_animal, id_utilisateur FROM empreinte")
            data_dw_image = dw_empreinte_cursor.fetchall()
            data_dw_image = pd.DataFrame(data_dw_image)
            data_ods_image = data_ods_image.rename(columns={'id': 'id_empreinte'})

            if data_dw_image.empty:
                pass
            else:
                # Comparaison et résultat des données à rajouter
                data_ods_image = data_ods_image[~data_ods_image['id_empreinte'].isin(data_dw_image['id_empreinte'])]

            if data_ods_image.empty:
                print("Aucune nouvelles données 'images' dans l'ODS !")
            else:
                for index, row in data_ods_image.iterrows():
                    query = "INSERT INTO empreinte (id_empreinte, coordonnee_empreinte, img_empreinte, date_empreinte, id_animal, id_utilisateur) VALUES (%s, %s, %s, %s, %s, %s)"
                    dw_empreinte_cursor.execute(query, (row['id_empreinte'], row['coordinates'], row['image_blob'], row['created_at'], row['id_animal'], row['user']))
            
                # Confirmation des transactions
                datawarehouse_conn.commit()
                print("Les informations 'images' dans ODS ont bien été inséré dans la base de données de Datawarehouse.")

    except Error as e:
        print(f"Erreur lors de la récupération des données depuis la base de données: {e}")
    finally:
        if ods_conn.is_connected():
            ods_image_cursor.close()
            ods_infos_especes_cursor.close()
            dw_empreinte_cursor.close()
            dw_animal_cursor.close()
            ods_conn.close()