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
from datetime import datetime, timedelta

@app.get("/api/soil-data/latest")
async def get_sensor():
    db = get_db_connection()
    if not db: 
        return {"moisture": 0, "status": "Offline", "timestamp": "N/A"}
    
    try:
        cursor = db.cursor(dictionary=True)
        # Mengambil data terbaru berdasarkan ID terbesar
        cursor.execute("SELECT moisture, created_at FROM sensor_logs ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            m = float(row['moisture'])
            
            # --- LOGIKA OTOMATISASI STATUS ---
            if m < 35:
                status_text = "Tanah Kering"
            elif 35 <= m <= 75:
                status_text = "Kondisi Optimal"
            else:
                status_text = "Tanah Basah"
            
            # --- PERBAIKAN JAM UPDATE (KONVERSI KE WIB) ---
            waktu_data = row['created_at']
            
            if waktu_data:
                # Menambah 7 jam untuk mengonversi UTC (Railway) ke WIB
                waktu_wib = waktu_data + timedelta(hours=7)
                jam_update = waktu_wib.strftime("%H:%M:%S")
            else:
                # Jika created_at kosong, gunakan waktu server saat ini + 7 jam
                jam_update = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S")
            
            return {
                "moisture": m,
                "status": status_text,
                "timestamp": jam_update
            }
            
        return {"moisture": 0, "status": "Data Kosong", "timestamp": "-"}
    except Exception as e:
        print(f"Error Sensor: {e}")
        # Jika error, kirim waktu saat ini (WIB) sebagai fallback
        waktu_now = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S")
        return {"moisture": 0, "status": "Error", "timestamp": waktu_now}
    finally:
        db.close()