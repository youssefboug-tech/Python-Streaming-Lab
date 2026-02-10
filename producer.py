"""
producer.py
----------------
This file contains the logic for a Producer that simulates a real-time data
stream by reading events from a CSV file and sending them one by one into a queue.
"""

import csv
import time
import random
from queue import Queue

class CSVProducer:
    def __init__(self, csv_path, q, delay=1.0):
        """
        PARAMETERS:
        - csv_path : path to the CSV file (str)
        - q        : queue acting as our 'topic'
        - delay    : time to wait between sending each row (float)
        """
        self.csv_path = csv_path
        self.q = q
        self.delay = delay

    def start(self):
        """
        This method should:
        1. Open the CSV file.
        2. Loop through each row.
        3. Print the row being sent.
        4. Push it into the queue.
        5. Sleep for 'delay' seconds to simulate streaming.
        """
        print(f"Producer: Lecture du fichier {self.csv_path}...")
        
        try:
            with open(self.csv_path, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    # Conversion optionnelle pour nettoyer les données avant l'envoi
                    # (ex: transformer le montant "100.50" en nombre 100.50)
                    if 'amount' in row:
                        row['amount'] = float(row['amount'])
                    
                    # 3. Print the row being sent
                    print(f"Producer -> Envoi : {row}")
                    
                    # 4. Push into queue
                    self.q.put(row)
                    
                    # 5. Sleep with random jitter (delay +/- 20%)
                    # Cela rend la simulation plus "humaine" ou "réseau réel"
                    jitter = random.uniform(0.8, 1.2)
                    time.sleep(self.delay * jitter)
            
            # Signal de fin (Poison Pill)
            # C'est important pour dire au consommateur "Il n'y a plus rien !"
            self.q.put(None)
            print("Producer: Fin du fichier. Signal d'arrêt envoyé.")

        except FileNotFoundError:
            print(f"Erreur : Le fichier {self.csv_path} est introuvable.")

# Debugging test
if __name__ == "__main__":
    """
    Run this file alone to test your producer.
    Expected behavior:
    - It should print rows from the CSV file every 'delay' seconds.
    """
    # Création d'une queue de test
    test_queue = Queue()
    
    # Création du producteur (assure-toi que transactions.csv est bien dans le dossier)
    # On met un délai court (0.5s) pour tester rapidement
    producer = CSVProducer("transactions.csv", test_queue, delay=0.5)
    
    # Lancement
    producer.start()