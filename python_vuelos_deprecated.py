import socket
import pyodbc
import time
import json
import requests
import threading

# --- CONFIGURACIÓN ---
HOST = 'localhost'
PORT = 30003
TIMEOUT_SESION = 1200  # 20 minutos (Para cerrar un vuelo histórico)
URL_JSON = "http://172.17.18.25/tar1090/data/aircraft.json" 

# --- CONFIGURACIÓN DE PISTAS ---
PISTAS_POLIGONOS = {
    '05L/23R': [
        (19.427908, -99.090562), (19.427414, -99.090241),
        (19.445763, -99.057591), (19.446234, -99.057880) 
    ],
    '05R/23L': [
        (19.426983, -99.086526), (19.426434, -99.086164),
        (19.445344, -99.052611), (19.445824, -99.052898)
    ]
}

# --- CONEXIÓN BASE DE DATOS ---
def get_db_connection():
    return pyodbc.connect(
        'DRIVER={FreeTDS};'
        'SERVER=172.16.2.125;'
        'DATABASE=DatosCIRUM;'
        'UID=admin_DB_DatosCIRUM;'
        'PWD=CIRUM/*4dm1n1str4t0r2026;'
        'TDS_Version=7.4;'
        'PORT=1433;'
    )

# --- MEMORIA GLOBAL ---
sesiones_activas = {}
cache_json_data = {} 

# --- HILO DE DATOS LIMPIOS (JSON) ---
def worker_actualizar_json():
    global cache_json_data
    while True:
        try:
            response = requests.get(URL_JSON, timeout=2)
            if response.status_code == 200:
                data = response.json()
                temp_cache = {}
                for avion in data.get('aircraft', []):
                    hex_code = avion.get('hex', '').upper()
                    if hex_code:
                        raw_alt = avion.get('alt_baro') or avion.get('alt_geom')
                        final_alt = 0 if str(raw_alt).lower() == 'ground' else (int(float(raw_alt)) if raw_alt else None)
                        
                        raw_head = avion.get('true_heading') or avion.get('track')
                        final_track = int(float(raw_head)) if raw_head else None
                        
                        final_speed = int(float(avion.get('gs', 0))) if avion.get('gs') else None

                        temp_cache[hex_code] = {
                            'lat': avion.get('lat'), 'lon': avion.get('lon'),
                            'track': final_track, 'alt': final_alt, 
                            'speed': final_speed, 'squawk': avion.get('squawk'),
                            'flight': avion.get('flight', '').strip()
                        }
                cache_json_data = temp_cache
        except Exception as e:
            pass # Silencioso para no ensuciar logs
        time.sleep(1) 

# --- GEOMETRÍA DE PISTAS ---
def punto_en_poligono(lat, lon, poligono):
    n = len(poligono)
    adentro = False
    p1_lat, p1_lon = poligono[0]
    for i in range(n + 1):
        p2_lat, p2_lon = poligono[i % n]
        if lon > min(p1_lon, p2_lon):
            if lon <= max(p1_lon, p2_lon):
                if lat <= max(p1_lat, p2_lat):
                    if p1_lon != p2_lon:
                        xinters = (lon - p1_lon) * (p2_lat - p1_lat) / (p2_lon - p1_lon) + p1_lat
                    if p1_lat == p2_lat or lat <= xinters:
                        adentro = not adentro
        p1_lat, p1_lon = p2_lat, p2_lon
    return adentro

def identificar_pista(lat, lon):
    if lat is None or lon is None: return None
    for nombre, esquinas in PISTAS_POLIGONOS.items():
        if punto_en_poligono(lat, lon, esquinas): return nombre
    return None

# --- MAIN HISTÓRICO ---
def main():
    threading.Thread(target=worker_actualizar_json, daemon=True).start()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    print(f"📡 Histórico: Conectando a {HOST}:{PORT}...")
    
    try:
        s.connect((HOST, PORT))
        print(f"✅ Conectado. Modo Sesión Activa (Timeout: {TIMEOUT_SESION}s) con Fusión JSON.")
        
        buffer_socket = ""
        
        while True:
            try:
                data = s.recv(4096).decode('utf-8')
            except:
                print("⚠ Conexión perdida. Reintentando...")
                time.sleep(5)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try: s.connect((HOST, PORT))
                except: pass
                continue

            buffer_socket += data
            
            while "\n" in buffer_socket:
                line, buffer_socket = buffer_socket.split("\n", 1)
                fields = line.split(',')
                
                if len(fields) < 22 or fields[0] != 'MSG':
                    continue

                hex_id = fields[4].strip().upper()
                ahora = time.time()

                # 1. Limpiar sesiones expiradas
                if hex_id in sesiones_activas:
                    tiempo_inactivo = ahora - sesiones_activas[hex_id]['last_seen']
                    if tiempo_inactivo > TIMEOUT_SESION:
                        print(f"⏱ Sesión histórica cerrada para {hex_id}.")
                        del sesiones_activas[hex_id]

                # 2. Extracción Híbrida de Datos (Socket + JSON)
                raw_callsign = fields[10].strip() or None
                fisico_en_tierra = 1 if fields[21].strip() != '0' else 0
                vr = int(fields[16]) if fields[16].strip() else None

                json_info = cache_json_data.get(hex_id, {})
                
                lat = json_info.get('lat') or (float(fields[14]) if fields[14].strip() else None)
                lon = json_info.get('lon') or (float(fields[15]) if fields[15].strip() else None)
                track = json_info.get('track') or (int(fields[13]) if fields[13].strip() else None)
                alt = json_info.get('alt')
                speed = json_info.get('speed')
                squawk = json_info.get('squawk') or (fields[17].strip() if fields[17].strip() else None)
                callsign = json_info.get('flight') or raw_callsign

                # Fallbacks seguros para BD
                if alt is None and fields[11].strip(): 
                    try: alt = int(fields[11])
                    except: pass
                if speed is None and fields[12].strip(): 
                    try: speed = int(fields[12])
                    except: pass

                # 3. Cálculo de Pista
                pista_actual = identificar_pista(lat, lon)

                # 4. Lógica de Guardado en BD (RegVuelosDUMP)
                if hex_id in sesiones_activas:
                    # UPDATE
                    db_id = sesiones_activas[hex_id]['db_id']
                    
                    query_update = """
                    UPDATE dbo.RegVuelosDUMP SET
                        Callsign = COALESCE(?, Callsign),
                        Altitude = COALESCE(?, Altitude),
                        GroundSpeed = COALESCE(?, GroundSpeed),
                        Track = COALESCE(?, Track),
                        Latitude = COALESCE(?, Latitude),
                        Longitude = COALESCE(?, Longitude),
                        VerticalRate = COALESCE(?, VerticalRate),
                        Squawk = COALESCE(?, Squawk),
                        IsOnGround = ?,
                        PistaProbable = COALESCE(?, PistaProbable),
                        RegistroFecha = GETDATE()
                    WHERE ID = ?
                    """
                    cursor.execute(query_update, (
                        callsign, alt, speed, track, lat, lon, vr, squawk, fisico_en_tierra, pista_actual, db_id
                    ))
                    conn.commit()
                    sesiones_activas[hex_id]['last_seen'] = ahora

                else:
                    # INSERT (Nuevo Vuelo en el registro)
                    if lat is not None:
                        query_insert = """
                        SET NOCOUNT ON; 
                        INSERT INTO dbo.RegVuelosDUMP 
                        (HexIdent, Callsign, Altitude, GroundSpeed, Track, Latitude, Longitude, VerticalRate, Squawk, IsOnGround, PistaProbable, RegistroFecha)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                        SELECT SCOPE_IDENTITY();
                        """
                        
                        cursor.execute(query_insert, (
                            hex_id, callsign, alt, speed, track, lat, lon, vr, squawk, fisico_en_tierra, pista_actual
                        ))
                        
                        nuevo_id = cursor.fetchval()
                        conn.commit()
                        
                        if nuevo_id:
                            sesiones_activas[hex_id] = {'db_id': nuevo_id, 'last_seen': ahora}
                            print(f"🆕 Histórico Iniciado: {hex_id} (ID: {nuevo_id})")

    except KeyboardInterrupt:
        print("\n🛑 Cerrando histórico...")
        conn.close()

if __name__ == "__main__":
    main()