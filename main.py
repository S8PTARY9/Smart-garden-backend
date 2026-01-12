import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime

# ========== KONFIGURASI DATABASE RAILWAY ==========
def get_db_connection():
    try:
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
app = FastAPI(title="Smart Rice Flowman API")

# Middleware untuk mengizinkan akses dari Flutter (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== SETUP MQTT (Koneksi ke Alat IoT) ==========
MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC_COMMAND = "smart_plowing/command"

def send_mqtt_command(data):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, 1883, 60)
        client.publish(MQTT_TOPIC_COMMAND, json.dumps(data))
        client.disconnect()
        return True
    except Exception as e:
        print(f"MQTT Error: {e}")
        return False

# Schema Request sesuai dengan data dari Flutter
class PlowingRequest(BaseModel):
    path: List[dict]
    device_id: str
    moisture_at_start: float
    total_distance: float

# ========== ENDPOINTS API ==========

# 1. Endpoint Utama (Menghindari {"detail":"Not Found"})
@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "Backend Smart Rice Flowman Siap!",
        "version": "1.0.0"
    }

# 2. Simpan Pola Baru ke plowing_history & Kirim ke MQTT
@app.post("/api/plow-path")
async def receive_path(request: PlowingRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Gagal menyambung ke database Railway")
    
    try:
        cursor = conn.cursor()
        # Query simpan ke tabel plowing_history
        query = """INSERT INTO plowing_history 
                   (path_data, total_distance, moisture_at_start) 
                   VALUES (%s, %s, %s)"""
        
        path_json = json.dumps(request.path)
        cursor.execute(query, (path_json, request.total_distance, request.moisture_at_start))
        conn.commit()
        
        # Kirim perintah pergerakan ke IoT via MQTT
        mqtt_payload = {
            "action": "START_PLOWING",
            "device_id": request.device_id,
            "path": request.path,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        send_mqtt_command(mqtt_payload)

        return {"status": "success", "message": "Pola disimpan di MySQL dan perintah MQTT terkirim!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# 3. Ambil Riwayat dari tabel plowing_history
@app.get("/api/plowing-history")
async def get_history():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY created_at DESC LIMIT 10")
        rows = cursor.fetchall()
        
        # Format data agar JSON path_data bisa dibaca Flutter
        for row in rows:
            if isinstance(row['path_data'], str):
                row['path_data'] = json.loads(row['path_data'])
            row['created_at'] = row['created_at'].isoformat() if row['created_at'] else None
        return rows
    except Exception as e:
        print(f"Error History: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# 4. Ambil Data Sensor Terbaru dari tabel sensor_logs
@app.get("/api/soil-data/latest")
async def get_latest_sensor():
    conn = get_db_connection()
    if not conn: return {"moisture": 0, "status": "Offline"}
    try:
        cursor = conn.cursor(dictionary=True)
        # Mengambil data terbaru berdasarkan kolom timestamp
        cursor.execute("SELECT moisture, timestamp FROM sensor_logs ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            return {
                "moisture": row['moisture'],
                "timestamp": row['timestamp'].strftime("%H:%M:%S"),
                "status": "Online"
            }
        return {"moisture": 0, "status": "Data Kosong"}
    except Exception as e:
        print(f"Error Sensor: {e}")
        return {"moisture": 0, "status": "Error"}
    finally:
        cursor.close()
        conn.close()