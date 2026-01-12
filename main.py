import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime

# ========== KONFIGURASI DATABASE RAILWAY ==========
def get_db_connection():
    try:
        # Mengambil data dari tab 'Variables' di Railway
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQLDATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306))
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error Database: {err}")
        return None

# ========== SETUP FASTAPI ==========
app = FastAPI(title="Smart Rice Flowman API - Final")

# Middleware CORS agar Flutter bisa mengakses API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== SETUP MQTT ==========
MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC_COMMAND = "smart_plowing/command"

def send_mqtt_command(data):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, 1883, 60)
        client.publish(MQTT_TOPIC_COMMAND, json.dumps(data))
        client.disconnect()
        return True
    except:
        return False

# Model data untuk menerima input dari Flutter
class PlowingRequest(BaseModel):
    path: List[dict]
    device_id: str
    moisture_at_start: float
    total_distance: float

# ========== ROUTES / ENDPOINTS ==========

# 1. Halaman Utama (Mencegah Detail Not Found)
@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "Backend Smart Rice Flowman Berhasil Terhubung!",
        "database": "Connected"
    }

# 2. Simpan Pola & Kirim MQTT (Tabel: plowing_history)
@app.post("/api/plow-path")
async def receive_path(request: PlowingRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database Connection Failed")
    
    try:
        cursor = conn.cursor()
        # Pastikan nama kolom sesuai dengan yang Anda buat manual
        query = """INSERT INTO plowing_history 
                   (path_data, total_distance, moisture_at_start) 
                   VALUES (%s, %s, %s)"""
        
        path_json = json.dumps(request.path)
        cursor.execute(query, (path_json, request.total_distance, request.moisture_at_start))
        conn.commit()
        
        # Kirim ke Hardware via MQTT
        mqtt_payload = {
            "action": "START_PLOWING",
            "device_id": request.device_id,
            "path": request.path
        }
        send_mqtt_command(mqtt_payload)

        return {"status": "success", "message": "Data disimpan ke Railway MySQL"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# 3. Ambil Riwayat (Tabel: plowing_history)
@app.get("/api/plowing-history")
async def get_history():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY created_at DESC LIMIT 15")
        rows = cursor.fetchall()
        
        for row in rows:
            if isinstance(row['path_data'], str):
                row['path_data'] = json.loads(row['path_data'])
            # Format tanggal agar Flutter tidak error
            if row['created_at']:
                row['created_at'] = row['created_at'].isoformat()
        return rows
    except:
        return []
    finally:
        cursor.close()
        conn.close()

# 4. Ambil Sensor Terkini (Tabel: sensor_logs)
@app.get("/api/soil-data/latest")
async def get_latest_sensor():
    conn = get_db_connection()
    if not conn: return {"moisture": 0, "status": "Offline"}
    try:
        cursor = conn.cursor(dictionary=True)
        # Mengambil data terakhir berdasarkan kolom timestamp/id
        cursor.execute("SELECT moisture, timestamp FROM sensor_logs ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            return {
                "moisture": row['moisture'],
                "timestamp": row['timestamp'].strftime("%H:%M:%S"),
                "status": "Online"
            }
        return {"moisture": 0, "status": "No Data"}
    finally:
        cursor.close()
        conn.close()