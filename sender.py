import requests
import time
import socket
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import customtkinter as ctk

load_dotenv()

def check_internet_connection():
    url = "https://chinatownlogistic.com"
    timeout = 1
    while True:
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200: return True
        except requests.ConnectionError: time.sleep(1)

def get_computer_name_and_time():
    computer_name = socket.gethostname()
    current_time = datetime.now()
    
    time_minus_5 = current_time - timedelta(minutes=5)
    formatted_time = time_minus_5.strftime("%H:%M:%S")
    formatted_date = time_minus_5.strftime("%Y-%m-%d")
    
    return computer_name, formatted_date, formatted_time

def send_telegram_message(message, bot_token, chat_id):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    
    response = requests.post(url, data=payload)
    
    if response.status_code == 200: return True
    else: return False

def get_table_name(computer_name):
    if computer_name == "VISTO": return "VIS"
    elif computer_name == "LO-NUESTRO": return "LON"
    elif computer_name == "SAN-JOSE": return "SAJ"
    else: raise ValueError(f"Nombre del computador no reconocido: {computer_name}")

def connect_and_insert_to_db(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            computer_name, fecha, hora_llegada = get_computer_name_and_time()
            
            table_name = get_table_name(computer_name)
            
            check_query = f"SELECT * FROM {table_name} WHERE fecha = %s AND primera = TRUE"
            cursor.execute(check_query, (fecha,))
            result = cursor.fetchone()

            if not result:
                insert_query = f"""
                    INSERT INTO {table_name} (fecha, hora_llegada, primera)
                    VALUES (%s, %s, TRUE)
                """
                cursor.execute(insert_query, (fecha, hora_llegada))
                connection.commit()

    except Error as e:
        print(f"Error al conectarse a la base de datos: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def center_window(window, width, height):
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    x = (screen_width // 2) - (width // 2)
    y = (screen_height // 2) - (height // 2)
    window.geometry(f"{width}x{height}+{x}+{y}")

def show_arrival_time_window(arrival_time):
    arrival_window = ctk.CTkToplevel()
    arrival_window.title("Hora de Llegada")
    arrival_window.resizable(False, False)

    center_window(arrival_window, 400, 200) 

    arrival_label = ctk.CTkLabel(arrival_window, text=f"La hora {arrival_time} ha sido tomada como su hora de llegada.")
    arrival_label.pack(pady=20)

    ok_button = ctk.CTkButton(arrival_window, text="Entendido", command=arrival_window.destroy, fg_color="red")
    ok_button.pack(pady=10) 

if __name__ == "__main__":
    if check_internet_connection():
        host = os.getenv("DB_HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        database = os.getenv("DB_NAME")

        connect_and_insert_to_db(host, user, password, database)
        computer_name, fecha, hora_llegada = get_computer_name_and_time()
        hora_llegada_am_pm = datetime.strptime(hora_llegada, "%H:%M:%S").strftime("%I:%M %p")
        bot_token = os.getenv("TELEGRAM_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        send_telegram_message(f"{computer_name} - {hora_llegada_am_pm}.", bot_token, chat_id)

        root = ctk.CTk() 
        root.withdraw()
        show_arrival_time_window(hora_llegada_am_pm)
        root.mainloop()
