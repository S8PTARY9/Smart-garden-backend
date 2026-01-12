import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime

# ========== CONFIGURASI DATABASE RAILWAY ==========
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

# ========== FASTAPI SETUP ==========
app = FastAPI(title="Smart Rice Flowman API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== MQTT SETUP ==========
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

# Schema untuk Request dari Flutter
class PlowingRequest(BaseModel):
    path: List[dict]
    device_id: str
    moisture_at_start: float
    total_distance: float

# ========== ENDPOINTS ==========

# 1. Simpan Pola & Kirim ke MQTT
@app.post("/api/plow-path")
async def receive_path(request: PlowingRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database tidak terhubung")
    
    try:
        cursor = conn.cursor()
        # Simpan ke tabel plowing_history
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
            "path": request.path
        }
        send_mqtt_command(mqtt_payload)

        return {"status": "success", "message": "Pola disimpan dan perintah dikirim!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# 2. Ambil Riwayat untuk Daftar di Flutter
@app.get("/api/plowing-history")
async def get_history():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plowing_history ORDER BY created_at DESC LIMIT 15")
        result = cursor.fetchall()
        
        for item in result:
            if isinstance(item['path_data'], str):
                item['path_data'] = json.loads(item['path_data'])
            item['created_at'] = item['created_at'].isoformat()
        return result
    except:
        return []
    finally:
        cursor.close()
        conn.close()

# 3. Ambil Data Sensor Terbaru dari sensor_logs
@app.get("/api/soil-data/latest")
async def get_latest_sensor():
    conn = get_db_connection()
    if not conn: return {"moisture": 0, "status": "Offline"}
    try:
        cursor = conn.cursor(dictionary=True)
        # Mengambil log terakhir dari tabel sensor_logs
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