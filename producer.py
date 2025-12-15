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
        self.csv_path = csv_path  
        self.q = q                
        self.delay = delay
    def start(self):
        """
        This method should:
        1. Open the CSV file.
        2. Loop through each row 
        3. Print the row being sent.
        4. Push it into the queue 
        5. Sleep for 'delay' seconds to simulate streaming.

        TODO:
        - Implement the streaming behavior.
        - Optional: Add random jitter to simulate irregular network delays.
        """
        with open(self.csv_path, mode='r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                print(f"Sending row: {row}")  
                self.q.put(row)  

                jitter = random.uniform(0, 0.5)  
                time.sleep(self.delay + jitter)  
      


# Debugging test

if __name__ == "__main__":
    test_queue = Queue()
    csv_file_path = "transactions.csv"  


    # Create the producer instance
    producer = CSVProducer(csv_file_path, test_queue)

    # Start the producer (this will print rows every 'delay' seconds)
    producer.start()
   
