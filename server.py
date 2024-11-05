import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from flask import Flask, render_template, request
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

def generate_arrival_difference_table(local, fecha_inicio, fecha_fin):
    engine = create_engine(f'mysql+mysqlconnector://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}',
        connect_args={"auth_plugin": "mysql_native_password"}
    )
    
    query = f"""
    SELECT fecha, hora_llegada
    FROM {local}
    WHERE fecha BETWEEN %s AND %s
    """
    
    df = pd.read_sql(query, engine, params=(fecha_inicio, fecha_fin))

    if df.empty:
        return "No se encontraron registros en el rango de fechas especificado."
    
    df['hora_llegada'] = df['hora_llegada'].dt.components['hours'].astype(str).str.zfill(2) + ':' + \
                         df['hora_llegada'].dt.components['minutes'].astype(str).str.zfill(2)
    df['hora_llegada'] = pd.to_datetime(df['hora_llegada'], format='%H:%M', errors='coerce').dt.strftime('%I:%M %p')
    
    df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%d %B del %Y')
    
    result_df = df[['fecha', 'hora_llegada']]
    return result_df

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        local = request.form['local']
        fecha_inicio = request.form['fecha_inicio']
        fecha_fin = request.form['fecha_fin']

        if not fecha_inicio or not fecha_fin:
            return render_template('index.html', error="Por favor, ingresa ambas fechas.")

        table_map = {
            "Visto": "VIS",
            "Lo Nuestro": "LON",
            "San Jose": "SAJ"
        }
        local_table = table_map.get(local)

        if local_table:
            try:
                result = generate_arrival_difference_table(local_table, fecha_inicio, fecha_fin)
                if isinstance(result, str):
                    return render_template('index.html', error=result)
                else:
                    table_html = result.to_html(
                        classes='table table-striped table-bordered',
                        index=False,
                        header=True,
                        justify='center'
                    ).replace('<th>fecha</th>', '<th>Fecha</th>').replace('<th>hora_llegada</th>', '<th>Hora llegada</th>')

                    return render_template('index.html', table=table_html)
            except mysql.connector.errors.DatabaseError as e:
                if e.errno == 1525:
                    return render_template('index.html', error="Por favor, ingresa fechas válidas.")
                else:
                    return render_template('index.html', error=f"Error de conexión a la base de datos: {str(e)}")
            except Exception as e:
                return render_template('index.html', error=str(e))
        else:
            return render_template('index.html', error="Selecciona un local válido.")
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)