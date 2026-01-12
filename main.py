import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# ========== KONFIGURASI DATABASE RAILWAY ==========
def get_db_connection():
    try:
        # Mengambil kredensial otomatis dari environment variables Railway
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
        print(f"DATABASE ERROR: {e}")
        return None

# ========== SETUP FASTAPI & CORS ==========
app = FastAPI(title="Smart Rice Flowman API Final")

# Mengizinkan Flutter (Android, iOS, Web) mengakses backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== SETUP MQTT ==========
MQTT_BROKER = "broker.hivemq.com"
MQTT_TOPIC = "smart_plowing/command"

def send_mqtt_command(payload):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, 1883, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
        return True
    except:
        return False

# Model Data Request dari Flutter
class PlowingRequest(BaseModel):
    path: List[dict]
    device_id: str
    moisture_at_start: float
    total_distance: float

# ========== ENDPOINTS API ==========

# 1. Root Endpoint (Untuk cek backend hidup/mati)
@app.get("/")
async def root():
    return {"status": "online", "message": "Backend Smart Rice Flowman Siap!"}

# 2. Simpan Pola & Kirim ke IoT (Tabel: plowing_history)
@app.post("/api/plow-path")
async def save_path(request: PlowingRequest):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Gagal terhubung ke database")
    
    try:
        cursor = db.cursor()
        # Query memasukkan data pola
        sql = """INSERT INTO plowing_history 
                 (path_data, total_distance, moisture_at_start) 
                 VALUES (%s, %s, %s)"""
        
        # Konversi list koordinat menjadi string JSON agar masuk ke LONGTEXT
        val = (json.dumps(request.path), request.total_distance, request.moisture_at_start)
        
        cursor.execute(sql, val)
        db.commit()
        
        # Kirim perintah ke alat via MQTT
        send_mqtt_command({
            "action": "START",
            "device": request.device_id,
            "path": request.path
        })
        
        return {"status": "success", "message": "Pola tersimpan di Database!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error Simpan: {str(e)}")
    finally:
        db.close()

# 3. Ambil Riwayat untuk Tabel Flutter (Tabel: plowing_history)
@app.get("/api/plowing-history")
async def get_history():
    db = get_db_connection()
    if not db: return []
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY id DESC LIMIT 20")
        rows = cursor.fetchall()
        
        # Format agar path_data kembali jadi List, bukan String
        for row in rows:
            if isinstance(row['path_data'], str):
                row['path_data'] = json.loads(row['path_data'])
            if row['created_at']:
                row['created_at'] = row['created_at'].isoformat()
        return rows
    except Exception as e:
        print(f"History Error: {e}")
        return []
    finally:
        db.close()

# 4. Ambil Data Sensor Terkini (Tabel: sensor_logs)
@app.get("/api/soil-data/latest")
async def get_latest_sensor():
    db = get_db_connection()
    # Jika DB mati, tetap kirim 200 OK agar status Flutter tidak "Disconnected"
    if not db: 
        return {"moisture": 0, "status": "DB Offline", "timestamp": "N/A"}
    
    try:
        cursor = db.cursor(dictionary=True)
        # Ambil 1 data terbaru dari sensor_logs
        cursor.execute("SELECT moisture, timestamp FROM sensor_logs ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            return {
                "moisture": row['moisture'],
                "timestamp": row['timestamp'].strftime("%H:%M:%S") if row['timestamp'] else "-",
                "status": "Online"
            }
        # Jika tabel kosong
        return {"moisture": 0, "status": "No Data", "timestamp": "-"}
    except:
        return {"moisture": 0, "status": "Error", "timestamp": "N/A"}
    finally:
        db.close()