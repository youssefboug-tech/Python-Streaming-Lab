"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system.
"""
import csv
import random

import time
from queue import Queue

class Consumer:
    def __init__(self, q):
        
        self.q=q
        self.running_total = 0  
        """
        PARAMETERS:
        - q : the shared queue (simulated topic)

        TODO:
        - Store the queue.
        - Initialize any state variables you may want later,
          e.g. a running total for stateful computations.
        """
        

    def start(self):
        """
        Main loop of the consumer.

        TODO:
        - Continuously call q.get() to receive events.
        - Print the event received.
        - Pass it to the process() function.
        """
        last_non_empty_time = time.time()  # Track the last time the queue was not empty

        while True:
            if not self.q.empty():
                event = self.q.get() 
                self.process(event)  
                last_non_empty_time = time.time()  
            else:
                
                if time.time() - last_non_empty_time > 60:
                    print("Queue has been empty for more than 1 minute. Exiting consumer loop.")
                    break  
            time.sleep(1)  
    

    def process(self, event):
        """
        TODO:
        - Simulate some processing time using time.sleep().
        - Transform fields (e.g., convert amount to float).
        - Implement optional filtering conditions.
        - Update any state (example: running total).
        """
        # Simulate some processing time
        time.sleep(1)  # Simulate processing delay (can be adjusted)

        # Access 'amount' directly from the list (assuming it's at index 2 in the list)
        try:
            amount = float(event[2])  # Convert the value at index 2 to float
        except ValueError:
            amount = 0  # If conversion fails, set amount to 0

        # Optional filtering: For example, only process events with positive amounts
        if amount > 0:
            print(f"Processing event with amount: {amount}")
            
            # Update the running total state variable
            self.running_total += amount
            print(f"Updated running total: {self.running_total}")
        else:
            print(f"Filtered out event with invalid or zero amount: {amount}")
   

# Debugging test

if __name__ == "__main__":
    """
    Run this alone to see consumer behavior.

    Note: It will block waiting for messages since no producer is running.
    """
    
    q = Queue()
    csv_path = "transactions.csv" 
    with open(csv_path, mode='r', newline='') as file:
            reader = csv.reader(file)
            x=1
            for row in reader:
                if x==1:
                    x=x-1
                else:    
                    print(f"Sending row: {row}")  
                    q.put(row)  

    

    consumer = Consumer(q)
    consumer.start()
