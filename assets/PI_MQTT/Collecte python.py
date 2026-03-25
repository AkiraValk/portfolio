import paho.mqtt.client as mqtt
import mysql.connector


# Configuration
BROKER = "test.mosquitto.org"
TOPIC = "IUT/Colmar2025/SAE2.04/Maison1"
DB_HOST = "10.252.3.39"  # Adresse de la VM MariaDB
DB_USER = "toto"
DB_PASS = "toto"
DB_NAME = "maison"

# File d'attente pour les messages en cas de déconnexion DB
message_queue = []

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def process_message(message):
    champs = dict(item.split("=") for item in message.split(","))
    required_fields = ["Id", "piece", "date", "time", "temp"]
    if not all(field in champs for field in required_fields):
        print(f"Message incomplet ou mal formaté : {message}")
        return False

    # Préparation de la date au format YYYY-MM-DD
    date = champs["date"].split("/")
    date_mysql = f"{date[2]}-{date[1]}-{date[0]}"

    # Préparation des valeurs pour la table data
    val = (champs["Id"], date_mysql, champs["time"], champs["temp"].replace(",", "."))
    # Préparation pour la table commune
    val_commune = (champs["Id"], champs["piece"])
    return (val, val_commune)

def insert_data(db, cursor, val, val_commune):
    try:
        # Insertion dans la table data
        sql = """ 
        INSERT INTO data (capteur_Id, date, heure, temp)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(sql, val)
        db.commit()

        # Insertion dans la table commune
        sql_commune = """
        INSERT INTO commune (capteur_Id, lieu)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE lieu = VALUES(lieu)
        """
        cursor.execute(sql_commune, val_commune)
        db.commit()
        return True
    except mysql.connector.Error as e:
        print(f"Erreur DB : {e}")
        db.rollback()
        return False

def process_queue(db, cursor):
    global message_queue
    while message_queue:
        val, val_commune = message_queue.pop(0)
        if not insert_data(db, cursor, val, val_commune):
            # Si échec, on remet le message en début de file
            message_queue.insert(0, (val, val_commune))
            break

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Message reçu : {message}")
    try:
        # Traitement du message
        result = process_message(message)
        if not result:
            return

        val, val_commune = result

        # Tentative de connexion à la DB si besoin
        try:
            db = connect_db()
            cursor = db.cursor()
        except Exception as e:
            print(f"Erreur connexion DB : {e}")
            message_queue.append((val, val_commune))
            return

        # Insertion des données
        if not insert_data(db, cursor, val, val_commune):
            message_queue.append((val, val_commune))
        else:
            process_queue(db, cursor)

        cursor.close()
        db.close()
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")

client = mqtt.Client()
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.subscribe(TOPIC)
client.loop_forever()
