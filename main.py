import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta

# ================= KONFIGURASI MQTT =================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "titan_flowman/pola_jalur"

# ================= KONFIGURASI DATABASE =================
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQLDATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306)),
            autocommit=True
        )
        return conn
    except Exception as e:
        print(f"Koneksi DB Gagal: {e}")
        return None

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Model data dari Flutter
class PlowingRequest(BaseModel):
    path: List[dict]
    total_distance: float

# ================= ENDPOINTS =================

@app.get("/")
async def root():
    return {"status": "online", "message": "Backend Smart Rice Flowman Aktif"}

# 1. Kirim & Simpan Pola
@app.post("/api/plow-path")
async def save_path(request: PlowingRequest):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    
    try:
        cursor = db.cursor()
        # Simpan ke MySQL
        sql = "INSERT INTO plowing_history (path_data, total_distance) VALUES (%s, %s)"
        val = (json.dumps(request.path), request.total_distance)
        cursor.execute(sql, val)
        
        # --- LOGIKA MQTT FINAL ---
        try:
            # Menggunakan CallbackAPIVersion untuk kompatibilitas paho-mqtt 2.0+
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1) 
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            
            payload = json.dumps({"path": request.path})
            
            # Menggunakan QoS 1 agar pesan dijamin sampai ke Broker
            result = client.publish(MQTT_TOPIC, payload, qos=1)
            
            # Memastikan pesan terkirim sebelum disconnect
            result.wait_for_publish() 
            client.disconnect()
            print(f"Berhasil Publish ke {MQTT_TOPIC} dengan QoS 1")
        except Exception as mqtt_err:
            print(f"Gagal MQTT: {mqtt_err}")

        return {"status": "success", "message": "Pola tersimpan & terkirim ke MQTT"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

# 2. Ambil Riwayat
@app.get("/api/plowing-history")
async def get_history():
    db = get_db_connection()
    if not db: return []
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY id DESC LIMIT 10")
        rows = cursor.fetchall()
        for r in rows:
            if isinstance(r['path_data'], str):
                r['path_data'] = json.loads(r['path_data'])
            if r['created_at']:
                r['created_at'] = r['created_at'].isoformat()
        return rows
    finally:
        db.close()

# 3. Ambil Sensor (Otomatis WIB)
@app.get("/api/soil-data/latest")
async def get_sensor():
    db = get_db_connection()
    if not db: 
        return {"moisture": 0, "status": "Offline", "timestamp": "N/A"}
    
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT moisture, created_at FROM sensor_logs ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            m = float(row['moisture'])
            
            # Logika Status Otomatis
            if m < 35:
                status_text = "Tanah Kering"
            elif 35 <= m <= 75:
                status_text = "Kondisi Optimal"
            else:
                status_text = "Tanah Basah"
            
            waktu_data = row['created_at']
            if waktu_data:
                # Konversi UTC ke WIB (+7 Jam)
                waktu_wib = waktu_data + timedelta(hours=7)
                jam_update = waktu_wib.strftime("%H:%M:%S")
            else:
                jam_update = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S")
            
            return {
                "moisture": m,
                "status": status_text,
                "timestamp": jam_update
            }
            
        return {"moisture": 0, "status": "Data Kosong", "timestamp": "-"}
    except Exception as e:
        print(f"Error Sensor: {e}")
        waktu_now = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S")
        return {"moisture": 0, "status": "Error", "timestamp": waktu_now}
    finally:
        db.close()