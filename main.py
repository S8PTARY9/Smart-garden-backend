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

app = FastAPI()

# Middleware CORS (Tetap sama)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Model data dari Flutter (Tetap sama)
class PlowingRequest(BaseModel):
    path: List[dict]
    total_distance: float
    
class SensorEntry(BaseModel):  # <--- PASTIKAN INI ADA
    moisture: float
    status: str

# ================= KONEKSI DATABASE =================
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

# ================= ENDPOINTS =================

@app.get("/")
async def root():
    return {"status": "online", "message": "Backend Smart Rice Flowman Aktif"}

# 1. KIRIM & SIMPAN POLA (DIPERBAIKI STABILITASNYA)
@app.post("/api/plow-path")
async def save_path(request: PlowingRequest):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    
    try:
        cursor = db.cursor()
        # A. SIMPAN KE MYSQL (Tetap sesuai fungsi lama Anda)
        sql = "INSERT INTO plowing_history (path_data, total_distance) VALUES (%s, %s)"
        val = (json.dumps(request.path), request.total_distance)
        cursor.execute(sql, val)
        
        # B. KIRIM KE MQTT (DIPERBAIKI AGAR TIDAK DISCONNECT)
        try:
            # Menggunakan API Version 1 untuk stabilitas library di Railway
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            
            payload = json.dumps({"path": request.path})
            
            # Publish dengan QoS 1
            result = mqtt_client.publish(MQTT_TOPIC, payload, qos=1)
            
            # MENUNGGU PROSES PUBLISH SELESAI (PENTING!)
            # Ini mencegah backend disconnect karena mematikan koneksi terlalu cepat
            result.wait_for_publish(timeout=2.0)
            
            mqtt_client.disconnect()
            print(f"MQTT Berhasil: {MQTT_TOPIC}")
        except Exception as mqtt_err:
            print(f"Logika MQTT bermasalah tapi DB aman: {mqtt_err}")

        return {"status": "success", "message": "Pola tersimpan & dikirim"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

# 2. AMBIL RIWAYAT (Fungsi Lama Tetap Sama)
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
        
# 3. HAPUS RIWAYAT BERDASARKAN ID
@app.delete("/api/plowing-history/{history_id}")
async def delete_history(history_id: int):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    
    try:
        cursor = db.cursor()
        
        # Cek apakah data dengan ID tersebut ada
        cursor.execute("SELECT id FROM plowing_history WHERE id = %s", (history_id,))
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Riwayat tidak ditemukan")
        
        # Hapus data
        cursor.execute("DELETE FROM plowing_history WHERE id = %s", (history_id,))
        
        return {
            "status": "success", 
            "message": f"Riwayat ID {history_id} berhasil dihapus"
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error saat menghapus riwayat: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

# 4. HAPUS SEMUA RIWAYAT (Versi Super Kuat)
@app.delete("/api/plowing-history/all")
async def delete_all_history():
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    
    try:
        # Gunakan autocommit agar perintah langsung dieksekusi tanpa antre
        db.autocommit = True
        cursor = db.cursor()
        
        # 1. Matikan proteksi (Sangat penting karena ada casting array/JSON di Laravel)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 2. Kosongkan tabel secara total
        # TRUNCATE akan mereset ID kembali ke 1 dan membersihkan kolom JSON path_data
        cursor.execute("TRUNCATE TABLE plowing_history")
        
        # 3. Hidupkan kembali proteksi
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        return {
            "status": "success", 
            "message": "Seluruh riwayat berhasil dibersihkan dan ID di-reset"
        }
    except Exception as e:
        print(f"Error fatal: {e}")
        # Jika TRUNCATE gagal, gunakan DELETE sebagai cadangan (fallback)
        try:
            cursor.execute("DELETE FROM plowing_history WHERE id > 0")
            db.commit()
            return {"status": "success", "message": "Riwayat dihapus dengan metode DELETE"}
        except:
            raise HTTPException(status_code=500, detail=f"Gagal total: {str(e)}")
    finally:
        db.close()

# 3. AMBIL SENSOR TERBARU (Fungsi Lama Tetap Sama dengan perbaikan jam WIB)
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
            if m < 35: status_text = "Tanah Kering"
            elif 35 <= m <= 75: status_text = "Kondisi Optimal"
            else: status_text = "Tanah Basah"
            
            # Jam Update WIB
            waktu_data = row['created_at']
            if waktu_data:
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
    finally:
        db.close()
        
# ==========================================================
# 1. ENDPOINT UNTUK ARDUINO (MENERIMA DATA)
# ==========================================================
@app.post("/api/soil-data/record")
async def record_sensor(data: SensorEntry):
    db = get_db_connection()
    if not db:
        raise HTTPException(status_code=500, detail="Database Offline")
    try:
        cursor = db.cursor()
        # Menyimpan data asli dari sensor ke tabel sensor_logs
        sql = "INSERT INTO sensor_logs (moisture, status, created_at) VALUES (%s, %s, NOW())"
        val = (data.moisture, data.status)
        cursor.execute(sql, val)
        return {"status": "success", "message": "Data Berhasil Dicatat"}
    except Exception as e:
        print(f"Gagal simpan sensor: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()