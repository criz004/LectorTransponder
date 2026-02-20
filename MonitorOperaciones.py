import socket
import pyodbc
import time
import json
import requests
import threading
from datetime import datetime

# --- CONFIGURACIÓN ---
HOST_SBS = 'localhost' # Puerto 30003 (Eventos)
PORT_SBS = 30003

# URL del JSON (Fuente de Calidad para Lat/Lon/Track)
URL_JSON = "http://172.17.18.25/tar1090/data/aircraft.json" 

TIEMPO_EXPIRACION = 60  # 1 min

# --- CONFIGURACIÓN DE PISTAS (POLÍGONOS EXACTOS) ---
PISTAS_POLIGONOS = {
    '05L/23R': [
        (19.427908, -99.090562),
        (19.427414, -99.090241),
        (19.445763, -99.057591),
        (19.446234, -99.057880) 
    ],
    '05R/23L': [
        (19.426983, -99.086526),
        (19.426434, -99.086164),
        (19.445344, -99.052611),
        (19.445824, -99.052898)
    ]
}

# --- CONEXIÓN DB ---
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
memoria_aviones = {} 
cache_json_data = {} 

# --- HILO DE ACTUALIZACIÓN JSON (BACKGROUND) ---
def worker_actualizar_json():
    """Consulta el aircraft.json cada segundo con LÓGICA DE PRIORIDAD DE RUMBO"""
    global cache_json_data
    while True:
        try:
            response = requests.get(URL_JSON, timeout=2)
            if response.status_code == 200:
                data = response.json()
                aircraft_list = data.get('aircraft', [])
                
                temp_cache = {}
                for avion in aircraft_list:
                    hex_code = avion.get('hex', '').upper()
                    if hex_code:
                        # 1. SANITIZACIÓN DE ALTITUD (FIX "ground")
                        raw_alt = avion.get('alt_baro')
                        if raw_alt is None: raw_alt = avion.get('alt_geom')
                        
                        final_alt = None
                        if str(raw_alt).lower() == 'ground':
                            final_alt = 0
                        else:
                            try: final_alt = int(float(raw_alt))
                            except: final_alt = None

                        # 2. SANITIZACIÓN DE RUMBO (FIX true_heading vs track)
                        # Prioridad: true_heading (nariz) > track (movimiento)
                        raw_heading = avion.get('true_heading')
                        if raw_heading is None:
                            raw_heading = avion.get('track')
                        
                        final_track = None
                        try: final_track = int(float(raw_heading))
                        except: final_track = None

                        # 3. VELOCIDAD (gs)
                        final_speed = None
                        try: final_speed = int(float(avion.get('gs', 0)))
                        except: final_speed = None

                        temp_cache[hex_code] = {
                            'lat': avion.get('lat'),
                            'lon': avion.get('lon'),
                            'track': final_track, # Aquí ya va el mejor valor posible
                            'alt': final_alt, 
                            'speed': final_speed,
                            'squawk': avion.get('squawk'),
                            'flight': avion.get('flight', '').strip()
                        }
                
                cache_json_data = temp_cache
            
        except Exception as e:
            print(f"⚠️ Error leyendo JSON: {e}")
        
        time.sleep(1) 

# --- FUNCIONES DE GEOMETRÍA ---
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
    for nombre_pista, esquinas in PISTAS_POLIGONOS.items():
        if punto_en_poligono(lat, lon, esquinas):
            return nombre_pista
    return None

# --- FUNCIONES DE LÓGICA ---
def analizar_squawk(squawk, hex_id):
    if squawk == '7700':
        print(f"🚨 ALERTA ROJA: Emergencia General en {hex_id}")
        return 'EMERGENCIA'
    elif squawk == '7600':
        return 'FALLA_RADIO'
    elif squawk == '7500':
        return 'SECUESTRO'
    return None

def limpiar_inactivos(cursor):
    try:
        query_clean = f"DELETE FROM dbo.EstadoAeropuerto WHERE DATEDIFF(second, UltimaActualizacion, GETDATE()) > {TIEMPO_EXPIRACION}"
        cursor.execute(query_clean)
        cursor.commit()
        
        ahora = time.time()
        borrar = [k for k, v in memoria_aviones.items() if (ahora - v['last_seen']) > TIEMPO_EXPIRACION]
        for k in borrar: del memoria_aviones[k]
    except Exception as e:
        pass # Silencioso para no ensuciar consola

def main():
    threading.Thread(target=worker_actualizar_json, daemon=True).start()
    print("🌍 Monitor Iniciado con ANTI-REBOTE (Delay 4s)")

    conn = get_db_connection()
    cursor = conn.cursor()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ultima_limpieza = time.time()

    try:
        s.connect((HOST_SBS, PORT_SBS))
        buffer_socket = ""
        
        while True:
            if time.time() - ultima_limpieza > 60:
                limpiar_inactivos(cursor)
                ultima_limpieza = time.time()

            try:
                data = s.recv(4096).decode('utf-8')
            except:
                time.sleep(5)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try: s.connect((HOST_SBS, PORT_SBS))
                except: pass
                continue

            buffer_socket += data
            while "\n" in buffer_socket:
                line, buffer_socket = buffer_socket.split("\n", 1)
                fields = line.split(',')
                if len(fields) < 22 or fields[0] != 'MSG': continue

                hex_id = fields[4].strip().upper()
                ahora_ts = time.time()

                # DATOS RAW
                raw_callsign = fields[10].strip() or None
                fisico_en_tierra = True if fields[21].strip() != '0' else False

                # DATOS JSON
                json_info = cache_json_data.get(hex_id, {})
                lat = json_info.get('lat') or (float(fields[14]) if fields[14].strip() else None)
                lon = json_info.get('lon') or (float(fields[15]) if fields[15].strip() else None)
                track = json_info.get('track') or (int(fields[13]) if fields[13].strip() else None)
                alt = json_info.get('alt')
                speed = json_info.get('speed')
                squawk = json_info.get('squawk') or (fields[17].strip() if fields[17].strip() else None)
                callsign = json_info.get('flight') or raw_callsign

                # --- CÁLCULO DE PISTA (CORRECCIÓN: SIEMPRE CALCULAR AQUÍ) ---
                pista_actual = identificar_pista(lat, lon)

                # --- LÓGICA DE ANTI-REBOTE (DEBOUNCING) ---
                if hex_id not in memoria_aviones:
                    memoria_aviones[hex_id] = {
                        'gnd_confirmado': fisico_en_tierra,
                        'estado_candidato': fisico_en_tierra,
                        'inicio_candidato': ahora_ts,
                        'estado_logico': 'EN_TIERRA' if fisico_en_tierra else 'EN_VUELO',
                        'last_seen': ahora_ts
                    }

                avion = memoria_aviones[hex_id]
                avion['last_seen'] = ahora_ts

                # Verificación de cambio de estado
                if fisico_en_tierra != avion['gnd_confirmado']:
                    if fisico_en_tierra == avion['estado_candidato']:
                        tiempo_espera = ahora_ts - avion['inicio_candidato']
                        
                        if tiempo_espera > TIEMPO_CONFIRMACION:
                            # ¡CONFIRMADO!
                            avion['gnd_confirmado'] = fisico_en_tierra
                            
                            # Lógica de impresión de eventos
                            if fisico_en_tierra:
                                avion['estado_logico'] = 'EN_TIERRA'
                                print(f"🛬 ATERRIZAJE CONFIRMADO: {callsign or hex_id} [Pista: {pista_actual or '?'}]")
                            else:
                                # Filtro extra de física para despegue
                                if (alt and alt > 200) or (speed and speed > 100):
                                    avion['estado_logico'] = 'EN_VUELO'
                                    print(f"🛫 DESPEGUE CONFIRMADO: {callsign or hex_id} [Pista: {pista_actual or '?'}]")
                                else:
                                    avion['gnd_confirmado'] = True 
                    else:
                        avion['estado_candidato'] = fisico_en_tierra
                        avion['inicio_candidato'] = ahora_ts
                else:
                    avion['estado_candidato'] = fisico_en_tierra
                    avion['inicio_candidato'] = ahora_ts

                # --- LÓGICA DE ESTADOS ESPECIALES ---
                if squawk:
                    emergencia = analizar_squawk(str(squawk), hex_id)
                    if emergencia: avion['estado_logico'] = emergencia

                # --- DB UPDATE ---
                estado_final = avion['estado_logico']
                
                # Ahora 'pista_actual' ya está definida arriba, así que no fallará
                if lat is not None:
                    query = """
                    MERGE dbo.EstadoAeropuerto AS target
                    USING (SELECT ? AS Hex) AS source ON (target.HexIdent = source.Hex)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            Callsign = COALESCE(?, target.Callsign),
                            Estado = ?,
                            Latitud = ?, Longitud = ?, Rumbo = ?, Velocidad = ?, Altitud = ?, Squawk = ?,
                            PistaProbable = COALESCE(?, target.PistaProbable),
                            HoraAterrizaje = CASE WHEN ? = 'EN_TIERRA' AND target.Estado <> 'EN_TIERRA' THEN GETDATE() ELSE target.HoraAterrizaje END,
                            HoraDespegue = CASE WHEN ? = 'EN_VUELO' AND target.Estado = 'EN_TIERRA' THEN GETDATE() ELSE target.HoraDespegue END,
                            UltimaActualizacion = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (HexIdent, Callsign, Estado, Latitud, Longitud, Rumbo, Velocidad, Altitud, Squawk, PistaProbable, HoraAterrizaje)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'EN_TIERRA' THEN GETDATE() ELSE NULL END);
                    """
                    cursor.execute(query, (
                        hex_id, 
                        callsign, estado_final, lat, lon, track, speed, alt, squawk, pista_actual, estado_final, estado_final,
                        hex_id, callsign, estado_final, lat, lon, track, speed, alt, squawk, pista_actual, estado_final
                    ))
                    conn.commit()

    except KeyboardInterrupt: print("\n🛑 Fin.")
    except Exception as e: print(f"❌ Error: {e}"); time.sleep(5)

if __name__ == "__main__":
    main()
