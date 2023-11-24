# SOLUCION MEJORADA

from flask import Flask, request, jsonify
import requests
import psycopg2
import random
import os
from dotenv import load_dotenv
from circuitbreaker import CircuitBreaker, CircuitBreakerError
from circuitbreaker import CircuitBreakerMonitor
from queue import Queue
import schedule
import time
from threading import Thread

app = Flask(__name__)
load_dotenv()

# Configure the circuit breaker
def my_fallback_function(arg):
    return "Fallback response"

circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60,
    name="my_circuit",
    fallback_function=my_fallback_function
)

# Configure the database connection
conn = psycopg2.connect(
    host=os.getenv('HOST'),
    database=os.getenv('DATABASE'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD')
)

monitor = CircuitBreakerMonitor()

# Configure the queue
failed_request_queue = Queue()

def handle_failed_request(url_request):
    print("failed", url_request)
    failed_request_queue.put(url_request)

def retry_failed_processes():
    while not failed_request_queue.empty():
        print("queue_size:", failed_request_queue.qsize())
        url_request = failed_request_queue.get()

        try:
            response = requests.get(url_request)
            print(response)
            if response.status_code == 200:
                anime_data = response.json()["data"]

                selected_data = {
                    "anime_id": anime_data["mal_id"],
                    "title": anime_data["title"],
                    "title_english": anime_data["title_english"],
                    "title_japanese": anime_data["title_japanese"]
                }

                with conn.cursor() as cur:
                    insert_query = """
                    INSERT INTO anime (id, title, title_english, title_japanese)
                    VALUES (%(anime_id)s, %(title)s, %(title_english)s, %(title_japanese)s);
                    """

                    cur.execute(insert_query, selected_data)
                    conn.commit()
            else:
                print("Else")
                if response.status_code != 404:
                    handle_failed_request(url_request)
        except Exception as e:
            print("Exception: ", e)
            handle_failed_request(url_request)
            

def execute_enqueued_requests():
        print("Executing enqueued requests ...")
        retry_failed_processes()
        print("Finishing execution of enqueued requests ...")

# Ruta para obtener detalles de un anime por ID
@app.route('/get_anime/<int:anime_id>', methods=['GET'])
def get_anime(anime_id):
    anime_data = get_anime_details(anime_id)

    if anime_data:
        if anime_data == "Fallback response":
            return "Fallback response"
        else:
            return jsonify(anime_data)
    else:
        return jsonify({"error": "Anime no encontrado"}), 404

# Function to get anime details from the database
def get_cached_anime_details(anime_id):
    with conn.cursor() as cur:
        select_query = "SELECT * FROM anime WHERE id = %s;"
        cur.execute(select_query, (anime_id,))
        anime_data = cur.fetchone()
        if anime_data:
            selected_data = {
                "anime_id": anime_data[0],
                "title": anime_data[1],
                "title_english": anime_data[2],
                "title_japanese": anime_data[3]
            }
            return selected_data
    return None

# Function to get anime details from the Jikan API and store in the database
@circuit_breaker
def get_anime_details(anime_id):
    # Uncomment this line in order to change the anime_id and work with randoms anime_id
    anime_id = random.randint(1, 1000)
    cached_data = get_cached_anime_details(anime_id)

    if cached_data:
        print('In cache', anime_id)
        return cached_data
    
    url = f'https://api.jikan.moe/v4/anime/{anime_id}/full'

    try:
        response = requests.get(url)

        if response.status_code == 200:
            anime_data = response.json()["data"]
            selected_data = {
                "anime_id": anime_id,
                "title": anime_data["title"],
                "title_english": anime_data["title_english"],
                "title_japanese": anime_data["title_japanese"]
            }

            # Insert the data into the database
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO anime (id, title, title_english, title_japanese)
                VALUES (%(anime_id)s, %(title)s, %(title_english)s, %(title_japanese)s);
                """
                cur.execute(insert_query, selected_data)
                conn.commit()

            return selected_data
        
        elif response.status_code == 429:
            print(f'Maximum number of requests per second reached, status code {response.status_code}')
            handle_failed_request(url)
            raise Exception("Simulated service failure!")

        else:
            print(f'Anime with id {anime_id} not found, status code {response.status_code}')
            raise Exception("Simulated service failure!")
    
    except CircuitBreakerError as e:
        print('Except CircuitBreaker')
        handle_failed_request(url)
        return jsonify({"status": "failure", "error": str(e)})


@app.route('/circuit_state')
def get_circuit_state():
    circuits = monitor.get_circuits()
    circuit_states = []

    for circuit in circuits:
        # Convert last_failure to a string representation
        last_failure_str = str(circuit.last_failure) if circuit.last_failure else None

        circuit_info = {
            "name": circuit.name,
            "state": circuit.state,
            "failure_count": circuit.failure_count,
            "last_failure": last_failure_str
        }

        circuit_states.append(circuit_info)

    return jsonify({"circuits": circuit_states})

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep for 1 second to avoid high CPU usage

def thread_execute_enqueued_requests():
    # Schedule the function to run every 15 seconds
    schedule.every(15).seconds.do(execute_enqueued_requests)

    # Create a separate thread for running the scheduler
    scheduler_thread = Thread(target=run_scheduler)
    scheduler_thread.start()

thread_execute_enqueued_requests()

if __name__ == '__main__':
    app.run(debug=True)
