"""
pipeline.py
----------------
This file launches BOTH producer and consumer using Python threads to simulate
a real streaming pipeline.
"""

import threading
from queue import Queue
from producer import CSVProducer
from consumer import Consumer

def main():
    # 1. Création de la Queue partagée (le "tuyau" entre les deux)
    shared_queue = Queue()

    # 2. Instanciation du Producteur et du Consommateur
    # On passe la même queue aux deux pour qu'ils puissent communiquer
    # On règle le délai à 0.5s pour que l'affichage soit fluide
    my_producer = CSVProducer("transactions.csv", shared_queue, delay=0.5)
    my_consumer = Consumer(shared_queue)

    # 3. Création des Threads
    # target=... indique quelle fonction le thread doit exécuter
    producer_thread = threading.Thread(target=my_producer.start)
    consumer_thread = threading.Thread(target=my_consumer.start)

    # 4. Lancement des Threads
    print("Pipeline: Lancement des threads...")
    
    # On lance le consommateur d'abord (il se met en attente)
    consumer_thread.start()
    # On lance le producteur ensuite (il commence à envoyer)
    producer_thread.start()

    # 5. Synchronisation (join)
    # Le programme principal attend ici que les deux threads aient fini leur travail
    # Sans ça, le script principal pourrait s'arrêter avant la fin du traitement.
    producer_thread.join()
    consumer_thread.join()

    print("Pipeline: Fin du traitement complet.")

if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()