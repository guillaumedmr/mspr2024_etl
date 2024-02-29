from insert_images_staging import insert_images_staging
from insert_infos_especes_staging import insert_infos_especes_staging
from transform_and_insert_ods import transform_and_insert_ods
from load_and_insert_datawarehouse import load_and_insert_datawarehouse

# Sources de données initiales
images_folder_path = "C:/Users/guill/OneDrive/Documents/mspr2024_etl/data/Mammifères"
csv_path = "C:/Users/guill/OneDrive/Documents/mspr2024_etl/data/infos_especes.csv"


# # [ONCE] Étape 1: Insertion des images du jeux de données de base dans le staging
# print('===========================')
# insert_images_staging(images_folder_path)

# # [ONCE] Étape 2: Insertion des informations du CSV de base dans le staging
# print('===========================')
# insert_infos_especes_staging(csv_path)

# [ETL APP] Étape 1: Redimensionnement et insertion des images dans la base de données ODS
print('===========================')
transform_and_insert_ods()

# [ETL APP] Étape 3: Récupération des données de ODS et remplissage des bonnes tables dans le DATAWAREHOUSE
print('===========================')
load_and_insert_datawarehouse()
print('===========================')