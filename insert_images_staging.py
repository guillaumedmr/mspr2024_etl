import os
import mysql.connector
from dotenv import load_dotenv
import base64

load_dotenv()

# Variables du .env
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME_STAGING')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

def image_to_base64(image_path):
    with open(image_path, 'rb') as image_file:
        base64_image = image_file.read()
    return base64_image

def insert_images_staging(main_folder):
    try:
        # Connexion à la base de données
        connection = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password)
        cursor = connection.cursor()

        # Parcourt tous les dossiers dans le dossier principal
        for foldername in os.listdir(main_folder):
            folder_path = os.path.join(main_folder, foldername)
            if os.path.isdir(folder_path):
                # Insère les images du dossier spécifié
                for filename in os.listdir(folder_path):
                    image_path = os.path.join(folder_path, filename)
                    if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                        # Converti l'image en base64
                        base64_image = image_to_base64(image_path)
                        
                        # Insère les données dans la table stg_image
                        query = "INSERT INTO stg_image (image_blob, espece) VALUES (%s, %s)"
                        cursor.execute(query, (base64_image, foldername))
        
        # Confirmation des transactions
        connection.commit()
        print("Les images ont été insérées avec succès dans la base de données de staging.")
    except Exception as e:
        print("Erreur lors de l'insertion des images dans la base de données de staging:", e)
    finally:
        # Fermeture de la connexion
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
