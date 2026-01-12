import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

# ========== CONFIGURASI DATABASE ==========
# Railway menyediakan variabel environment secara otomatis
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQLDATABASE'),
            port=os.getenv('MYSQLPORT')
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error Database: {err}")
        return None

# ========== FASTAPI SETUP ==========
app = FastAPI(title="Smart Rice Flowman API")

# Agar Flutter Web/Android bisa mengakses API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== MQTT SETUP ==========
MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC_COMMAND = "smart_plowing/command"
MQTT_TOPIC_SENSOR = "smart_plowing/sensor"

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT Broker with result code {rc}")
    client.subscribe(MQTT_TOPIC_SENSOR)

def on_message(client, userdata, msg):
    try:
        # Menerima data dari Wemos: {"moisture": 75.5}
        data = json.loads(msg.payload.decode())
        moisture = data.get('moisture')
        
        if moisture is not None:
            status = "Lembab" if moisture < 80 else "Kering"
            
            # Simpan ke MySQL Railway
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                query = "INSERT INTO sensor_logs (moisture, status) VALUES (%s, %s)"
                cursor.execute(query, (moisture, status))
                conn.commit()
                cursor.close()
                conn.close()
                print(f"Data sensor berhasil disimpan: {moisture}%")
    except Exception as e:
        print(f"Gagal memproses pesan MQTT: {e}")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, 1883, 60)
mqtt_client.loop_start()

# ========== SCHEMAS (Model Data) ==========
class Point(BaseModel):
    x: int
    y: int

class PathRequest(BaseModel):
    path: List[Point]
    total_distance: float

# ========== ENDPOINTS API ==========

@app.get("/")
def home():
    return {"status": "online", "message": "Backend Smart Rice Flowman Siap!"}

# Endpoint untuk Flutter mengirim pola
@app.post("/api/plow-path")
async def receive_path(request: PathRequest):
    # 1. Konversi list objek menjadi list dict/json
    path_data = [p.dict() for p in request.path]
    
    # 2. Kirim koordinat ke Wemos via MQTT
    mqtt_client.publish(MQTT_TOPIC_COMMAND, json.dumps(path_data))
    
    # 3. Simpan ke MySQL Railway
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database tidak terkoneksi")
    
    try:
        cursor = conn.cursor()
        query = "INSERT INTO plowing_history (path_data, total_distance) VALUES (%s, %s)"
        # path_data diubah ke string JSON agar sesuai tipe kolom json di MySQL
        cursor.execute(query, (json.dumps(path_data), request.total_distance))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Pola berhasil dikirim ke alat dan disimpan ke riwayat"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint untuk Flutter mengambil riwayat pembajakan
@app.get("/api/plowing-history")
def get_history():
    conn = get_db_connection()
    if not conn: return []
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM plowing_history ORDER BY created_at DESC LIMIT 10")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

# Endpoint untuk Flutter mengambil data sensor terbaru
@app.get("/api/soil-data/latest")
def get_latest_sensor():
    conn = get_db_connection()
    if not conn: return {"moisture": 0, "status": "Error DB"}
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM sensor_logs ORDER BY created_at DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result if result else {"moisture": 0, "status": "Belum ada data"}