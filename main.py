import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime

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

# 1. Kirim & Simpan Pola (Tabel: plowing_history)
@app.post("/api/plow-path")
async def save_path(request: PlowingRequest):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    
    try:
        cursor = db.cursor()
        # Sesuai tabel Anda: id, path_data, total_distance, created_at
        sql = "INSERT INTO plowing_history (path_data, total_distance) VALUES (%s, %s)"
        val = (json.dumps(request.path), request.total_distance)
        
        cursor.execute(sql, val)
        
        # Kirim MQTT ke Alat
        try:
            client = mqtt.Client()
            client.connect("broker.hivemq.com", 1883, 60)
            client.publish("smart_plowing/command", json.dumps({"path": request.path}))
            client.disconnect()
        except:
            pass

        return {"status": "success", "message": "Pola tersimpan"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

# 2. Ambil Riwayat (Tabel: plowing_history)
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

# 3. Ambil Sensor (Tabel: sensor_logs)
@app.get("/api/soil-data/latest")
async def get_sensor():
    db = get_db_connection()
    if not db: return {"moisture": 0, "status": "Offline"}
    try:
        cursor = db.cursor(dictionary=True)
        # Sesuai tabel Anda: id, moisture, status, created_at
        cursor.execute("SELECT moisture, status, created_at FROM sensor_logs ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            return {
                "moisture": row['moisture'],
                "status": row['status'],
                "timestamp": row['created_at'].strftime("%H:%M:%S") if row['created_at'] else "-"
            }
        return {"moisture": 0, "status": "No Data", "timestamp": "-"}
    finally:
        db.close()