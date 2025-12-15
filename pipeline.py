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
    """
    TODO:
    - Create the shared queue.
    - Instantiate the producer and consumer.
    - Create a thread for each.
    - Start both threads.
    - Optionally join them so the program runs continuously.

    The students should run:
        python pipeline.py
    and observe live streaming behavior.
    """
    q = Queue()

    producer = CSVProducer(csv_path='transactions.csv', q=q)  # Adjust CSV path and delay as needed
    consumer = Consumer(q=q)

    producer_thread = threading.Thread(target=producer.start)
    consumer_thread = threading.Thread(target=consumer.start)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()



if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()
