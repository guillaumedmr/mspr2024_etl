from insert_images_staging import insert_images_staging
from insert_infos_especes_staging import insert_infos_especes_staging
from transform_and_insert_ods import transform_and_insert_ods
from load_and_insert_datawarehouse import load_and_insert_datawarehouse

# Sources de données initiales
images_folder_path = "C:/Users/guill/OneDrive/Documents/MSPR ETL/data/Mammifères"
csv_path = "C:/Users/guill/OneDrive/Documents/MSPR ETL/data/infos_especes.csv"


# # [ONCE] Étape 1: Insertion des images du jeux de données de base dans le staging
# print("Début de l'insertion des images dans la base de données de staging...")
# insert_images_staging(images_folder_path)

# # [ONCE] Étape 2: Insertion des informations du CSV de base dans le staging
# insert_infos_especes_staging(csv_path)

# # [ETL APP] Étape 1: Redimensionnement et insertion des images dans la base de données ODS
# print("Début du traitement et de l'insertion des images dans la base de données ODS...")
# transform_and_insert_ods()

# [ETL APP] Étape 3: Récupération des données de ODS et remplissage des bonnes tables dans le DATAWAREHOUSE
load_and_insert_datawarehouse()