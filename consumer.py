"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system.
"""

import time
from queue import Queue,Empty

class Consumer:
    def __init__(self, q):
        """
        PARAMETERS:
        - q : the shared queue (simulated topic)
        """
        self.q = q
        # Variable d'état pour garder le total des montants
        self.running_total = 0.0
        self.transaction_count = 0

    def start(self):
        """
        Main loop of the consumer.
        """
        print("Consumer: Démarrage et attente de messages...")
        
        while True:
            try:
                # On attend une donnée pendant 60 secondes maximum
                event = self.q.get(timeout=60)

            except Empty:
                # Si 60s passent sans rien recevoir, on rentre ici
                print("Consumer: Aucune donnée reçue depuis 1 minute. Arrêt automatique.")
                break
            
            # --- Le reste du code ne change pas ---
            
            # Vérification du signal d'arrêt (Poison Pill)
            if event is None:
                print(f"Consumer: Signal d'arrêt reçu. Total final: {self.running_total:.2f}")
                break
            
            # On traite l'événement
            self.process(event)


    def process(self, event):
        """
        Simulate processing time and update state.
        """
        # Simulation d'un traitement complexe (ex: appel API, écriture DB...)
        time.sleep(0.2) 
        
        # Récupération du montant (déjà converti en float par le producer normalement)
        amount = event.get('amount', 0.0)
        t_id = event.get('transaction_id', 'UNKNOWN')
        
        # Mise à jour de l'état (Stateful Processing)
        self.running_total += amount
        self.transaction_count += 1
        
        # Affichage du résultat
        # On affiche le montant actuel ET le total cumulé
        print(f"Consumer -> Traité ID:{t_id} | Montant: {amount} | Total Cumulé: {self.running_total:.2f}")


# Debugging test
if __name__ == "__main__":
    """
    Run this alone to see consumer behavior.
    """
    q = Queue()
    consumer = Consumer(q)
    
    # Pour tester, on doit simuler l'arrivée de messages manuellement
    print("TEST: Injection de 3 transactions factices...")
    q.put({'transaction_id': 'TEST1', 'amount': 100.0})
    q.put({'transaction_id': 'TEST2', 'amount': 50.0})
    q.put({'transaction_id': 'TEST3', 'amount': 25.5})
    
    # Important : On doit aussi envoyer le signal de fin pour le test !
    
    consumer.start()