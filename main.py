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
        # Mengambil kredensial dari tab Variables Railway
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQLDATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306)),
            connect_timeout=10
        )
        return conn
    except Exception as e:
        print(f"DATABASE CONNECTION ERROR: {e}")
        return None

# ========== SETUP FASTAPI ==========
app = FastAPI(title="Smart Rice Flowman API")

# PENTING: Mengizinkan Flutter mengakses backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== SETUP MQTT (Untuk Alat IoT) ==========
MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC = "smart_plowing/command"

def send_mqtt(payload):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, 1883, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
    except:
        pass

# Model Data dari Flutter
class PlowingData(BaseModel):
    path: List[dict]
    device_id: str
    moisture_at_start: float
    total_distance: float

# ========== ROUTES ==========

@app.get("/")
async def health_check():
    return {"status": "online", "message": "Backend Smart Rice Flowman Siap!"}

# 1. Simpan Pola & Kirim MQTT (Tabel: plowing_history)
@app.post("/api/plow-path")
async def receive_path(request: PlowingData):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Gagal terhubung ke Database Railway")
    
    try:
        cursor = db.cursor()
        # PASTIKAN NAMA KOLOM INI SAMA DENGAN TABEL MANUAL ANDA
        sql = "INSERT INTO plowing_history (path_data, total_distance, moisture_at_start) VALUES (%s, %s, %s)"
        val = (json.dumps(request.path), request.total_distance, request.moisture_at_start)
        
        cursor.execute(sql, val)
        db.commit()
        
        # Kirim perintah ke traktor
        send_mqtt({
            "action": "START",
            "device": request.device_id,
            "coords": request.path
        })
        
        return {"status": "success", "message": "Data tersimpan di MySQL"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error Simpan: {str(e)}")
    finally:
        db.close()

# 2. Ambil Riwayat (Tabel: plowing_history)
@app.get("/api/plowing-history")
async def get_history():
    db = get_db_connection()
    if not db: return []
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY id DESC LIMIT 15")
        result = cursor.fetchall()
        for r in result:
            if isinstance(r['path_data'], str):
                r['path_data'] = json.loads(r['path_data'])
            if r['created_at']:
                r['created_at'] = r['created_at'].isoformat()
        return result
    except:
        return []
    finally:
        db.close()

# 3. Ambil Sensor Terakhir (Tabel: sensor_logs)
@app.get("/api/soil-data/latest")
async def get_sensor():
    db = get_db_connection()
    if not db: return {"moisture": 0, "status": "Offline"}
    try:
        cursor = db.cursor(dictionary=True)
        # Ambil 1 data terbaru dari tabel sensor_logs
        cursor.execute("SELECT moisture, timestamp FROM sensor_logs ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            return {
                "moisture": row['moisture'],
                "timestamp": row['timestamp'].strftime("%H:%M:%S") if row['timestamp'] else "N/A",
                "status": "Online"
            }
        return {"moisture": 0, "status": "No Data"}
    finally:
        db.close()