import socket
import pyodbc
import time
import requests
import threading
import logging
from datetime import datetime

# --- CONFIGURACIÓN DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("monitor_radar.log", encoding='utf-8'), 
        logging.StreamHandler() 
    ]
)

# --- CONFIGURACIÓN ---
HOST_SBS = 'localhost' 
PORT_SBS = 30003
URL_JSON = "http://172.17.18.238/tar1090/data/aircraft.json" 

TIEMPO_EXPIRACION = 20  
TIEMPO_CONFIRMACION = 4

# --- PISTAS ---
PISTAS_POLIGONOS = {
    '05L/23R': [(19.427908, -99.090562), (19.427414, -99.090241), (19.445763, -99.057591), (19.446234, -99.057880)],
    '05R/23L': [(19.426983, -99.086526), (19.426434, -99.086164), (19.445344, -99.052611), (19.445824, -99.052898)]
}

def get_db_connection():
    return pyodbc.connect(
        'DRIVER={FreeTDS};SERVER=172.16.2.125;DATABASE=DatosCIRUM;UID=admin_DB_DatosCIRUM;PWD=CIRUM/*4dm1n1str4t0r2026;TDS_Version=7.4;PORT=1433;'
    )

# --- MEMORIAS GLOBALES ---
memoria_aviones = {} 
cache_socket_data = {} # <--- EL ASISTENTE GUARDA SUS DATOS AQUÍ

def worker_leer_socket():
    """Lee el puerto 30003 en segundo plano y guarda lo que encuentre en el cache."""
    global cache_socket_data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    while True:
        try:
            s.connect((HOST_SBS, PORT_SBS))
            buffer_socket = ""
            logging.info("🔌 Asistente conectado al puerto 30003 (CSV).")
            
            while True:
                data = s.recv(4096).decode('utf-8')
                if not data: break
                
                buffer_socket += data
                while "\n" in buffer_socket:
                    line, buffer_socket = buffer_socket.split("\n", 1)
                    fields = line.split(',')
                    if len(fields) < 22 or fields[0] != 'MSG': continue

                    hex_id = fields[4].strip().upper()
                    if not hex_id: continue

                    if hex_id not in cache_socket_data:
                        cache_socket_data[hex_id] = {}
                    
                    # Guardamos los datos puros del socket
                    avion_s = cache_socket_data[hex_id]
                    avion_s['last_seen'] = time.time()
                    
                    if fields[10].strip(): avion_s['callsign'] = fields[10].strip()
                    if fields[14].strip(): avion_s['lat'] = float(fields[14])
                    if fields[15].strip(): avion_s['lon'] = float(fields[15])
                    if fields[13].strip(): avion_s['track'] = int(float(fields[13]))
                    if fields[11].strip(): avion_s['alt'] = int(float(fields[11]))
                    if fields[12].strip(): avion_s['speed'] = int(float(fields[12]))
                    if fields[17].strip(): avion_s['squawk'] = fields[17].strip()
                    avion_s['gnd_csv'] = True if fields[21].strip() != '0' else False

        except Exception as e:
            logging.warning(f"⚠️ Asistente 30003 desconectado: {e}")
            time.sleep(5)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# --- FUNCIONES AUXILIARES ---
def punto_en_poligono(lat, lon, poligono):
    if lat is None or lon is None: return False
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
                    if p1_lat == p2_lat or lat <= xinters: adentro = not adentro
        p1_lat, p1_lon = p2_lat, p2_lon
    return adentro

def identificar_pista(lat, lon):
    if lat is None or lon is None: return None
    for nombre_pista, esquinas in PISTAS_POLIGONOS.items():
        if punto_en_poligono(lat, lon, esquinas): return nombre_pista
    return None

def analizar_squawk(squawk, hex_id):
    if squawk == '7700':
        logging.error(f"🚨 EMERGENCIA (7700) en Hex: {hex_id}")
        return 'EMERGENCIA'
    elif squawk == '7600': return 'FALLA_RADIO'
    elif squawk == '7500': return 'SECUESTRO'
    return None

def determinar_tipo_aeronave(categoria, callsign):
    if not callsign: callsign = ""
    else: callsign = callsign.strip().upper()
    
    if categoria == 'A7': return 'HELICOPTERO'
    if callsign.startswith(('FAM', 'ANX', 'GN', 'SDN', 'PFP', 'XAM', 'XBN')): return 'MILITAR'
    if callsign.startswith(('MAS', 'UCG', 'LCO', 'CKS', 'ABX', 'TPA', 'FDX', 'UPS')): return 'CARGO'
    elif categoria == 'A1': return 'LIGERO'
    if not callsign and not categoria:
        return 'DESCONOCIDO'
        
    return 'COMERCIAL'

def main():
    threading.Thread(target=worker_leer_socket, daemon=True).start()
    logging.info("🌍 Monitor Iniciado: JSON es el Jefe, Socket 30003 es el Asistente.")

    conn = get_db_connection()
    cursor = conn.cursor()
    ultima_limpieza = time.time()

    try:
        while True:
            ahora_ts = time.time()

            # 1. Limpieza de base de datos y memorias
            if ahora_ts - ultima_limpieza > 60:
                try:
                    cursor.execute(f"DELETE FROM dbo.EstadoAeropuerto WHERE DATEDIFF(second, UltimaActualizacion, GETDATE()) > {TIEMPO_EXPIRACION}")
                    conn.commit() # Asegúrate de usar conn.commit() o cursor.commit() de forma consistente
                except Exception as e: 
                    if '08S01' in str(e):
                        logging.warning("⚠️ Omitiendo limpieza de BD: Conexión caída.")
                    else:
                        logging.error(f"⚠️ Error en limpieza de BD: {e}")
                
                borrar_mem = [k for k, v in memoria_aviones.items() if (ahora_ts - v['last_seen']) > TIEMPO_EXPIRACION]
                for k in borrar_mem: del memoria_aviones[k]
                
                borrar_csv = [k for k, v in cache_socket_data.items() if (ahora_ts - v.get('last_seen', 0)) > TIEMPO_EXPIRACION]
                for k in borrar_csv: del cache_socket_data[k]
                
                ultima_limpieza = ahora_ts

            # 2. EL JEFE: Descargar datos de JSON
            dict_json = {}
            try:
                response = requests.get(URL_JSON, timeout=2)
                if response.status_code == 200:
                    json_list = response.json().get('aircraft', [])
                    dict_json = {a.get('hex', '').upper(): a for a in json_list if a.get('hex')}
            except Exception as e:
                logging.warning(f"Error leyendo JSON (Jefe): {e}")

            # 3. CONSOLIDAR: Unir aviones del Jefe y del Asistente
            todos_los_hex = set(dict_json.keys()).union(set(cache_socket_data.keys()))

            # 4. PROCESAR CADA AVIÓN
            for hex_id in todos_los_hex:
                j_data = dict_json.get(hex_id, {})
                s_data = cache_socket_data.get(hex_id, {})

                # LÓGICA DE PRIORIDAD: Si el Jefe (JSON) lo tiene, lo usamos. Si no, preguntamos al Asistente (Socket).
                
                # Callsign
                raw_callsign = j_data.get('flight', '').strip()
                callsign = raw_callsign if raw_callsign else s_data.get('callsign')

                # Coordenadas
                lat = j_data.get('lat') if j_data.get('lat') is not None else s_data.get('lat')
                lon = j_data.get('lon') if j_data.get('lon') is not None else s_data.get('lon')

                # Altitud (Procesando el string 'ground' de tar1090)
                alt = None
                raw_alt = j_data.get('alt_baro') or j_data.get('alt_geom')
                if raw_alt is not None:
                    alt = 0 if str(raw_alt).lower() == 'ground' else int(float(raw_alt))
                else:
                    alt = s_data.get('alt')

                # Rumbo / Track
                track = None
                raw_track = j_data.get('true_heading') or j_data.get('track')
                if raw_track is not None: track = int(float(raw_track))
                else: track = s_data.get('track')

                # Velocidad
                speed = None
                raw_speed = j_data.get('gs')
                if raw_speed is not None: speed = int(float(raw_speed))
                else: speed = s_data.get('speed')

                # Squawk
                squawk = j_data.get('squawk') or s_data.get('squawk')

                # Metadata exclusiva de JSON
                matricula = j_data.get('registration')
                modelo = j_data.get('type')
                categoria_adsb = j_data.get('category')
                tipo_aeronave = determinar_tipo_aeronave(categoria_adsb, callsign)

                # ¿Está en tierra? (Si JSON dice 'ground', o si altitud es 0, o fallback al socket)
                fisico_en_tierra = False
                if str(j_data.get('alt_baro')).lower() == 'ground':
                    fisico_en_tierra = True
                elif s_data.get('gnd_csv') is True:
                    fisico_en_tierra = True

                pista_actual = identificar_pista(lat, lon)

                # --- MÁQUINA DE ESTADOS (ANTI-REBOTE) ---
                if hex_id not in memoria_aviones:
                    memoria_aviones[hex_id] = {
                        'gnd_confirmado': fisico_en_tierra, 'estado_candidato': fisico_en_tierra,
                        'inicio_candidato': ahora_ts, 'estado_logico': 'EN_TIERRA' if fisico_en_tierra else 'EN_VUELO',
                        'last_seen': ahora_ts
                    }

                avion = memoria_aviones[hex_id]
                avion['last_seen'] = ahora_ts

                if fisico_en_tierra != avion['gnd_confirmado']:
                    if fisico_en_tierra == avion['estado_candidato']:
                        tiempo_espera = ahora_ts - avion['inicio_candidato']
                        if tiempo_espera > TIEMPO_CONFIRMACION:
                            avion['gnd_confirmado'] = fisico_en_tierra
                            if fisico_en_tierra:
                                avion['estado_logico'] = 'EN_TIERRA'
                                logging.info(f"🛬 ATERRIZAJE: {callsign or hex_id} [Pista: {pista_actual or '?'}]")
                            else:
                                if (alt and alt > 200) and (speed and speed > 100):
                                    avion['estado_logico'] = 'EN_VUELO'
                                    logging.info(f"🛫 DESPEGUE: {callsign or hex_id} [Pista: {pista_actual or '?'}]")
                                else:
                                    avion['gnd_confirmado'] = True 
                    else:
                        avion['estado_candidato'] = fisico_en_tierra
                        avion['inicio_candidato'] = ahora_ts
                else:
                    avion['estado_candidato'] = fisico_en_tierra
                    avion['inicio_candidato'] = ahora_ts

                if squawk:
                    emergencia = analizar_squawk(str(squawk), hex_id)
                    if emergencia: avion['estado_logico'] = emergencia
                
                estado_final = avion['estado_logico']
                
                if lat is None and lon is None and not callsign:
                    estado_final = 'DESCONOCIDO'
                    tipo_aeronave = 'DESCONOCIDO'

                # --- ACTUALIZAR BASE DE DATOS ---
                query = """
                MERGE dbo.EstadoAeropuerto AS target
                USING (SELECT ? AS Hex) AS source ON (target.HexIdent = source.Hex)
                WHEN MATCHED THEN
                    UPDATE SET 
                        Callsign = COALESCE(?, target.Callsign), Estado = ?, Latitud = ?, Longitud = ?, 
                        Rumbo = ?, Velocidad = ?, Altitud = ?, Squawk = ?, PistaProbable = COALESCE(?, target.PistaProbable),
                        TipoAeronave = ?, Matricula = COALESCE(?, target.Matricula), Modelo = COALESCE(?, target.Modelo),
                        HoraAterrizaje = CASE WHEN ? = 'EN_TIERRA' AND target.Estado <> 'EN_TIERRA' THEN GETDATE() ELSE target.HoraAterrizaje END,
                        HoraDespegue = CASE WHEN ? = 'EN_VUELO' AND target.Estado = 'EN_TIERRA' THEN GETDATE() ELSE target.HoraDespegue END,
                        UltimaActualizacion = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (HexIdent, Callsign, Estado, Latitud, Longitud, Rumbo, Velocidad, Altitud, Squawk, PistaProbable, TipoAeronave, Matricula, Modelo, HoraAterrizaje)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'EN_TIERRA' THEN GETDATE() ELSE NULL END);
                """
                try:
                    cursor.execute(query, (
                        hex_id, callsign, estado_final, lat, lon, track, speed, alt, squawk, pista_actual, tipo_aeronave, matricula, modelo, estado_final, estado_final,
                        hex_id, callsign, estado_final, lat, lon, track, speed, alt, squawk, pista_actual, tipo_aeronave, matricula, modelo, estado_final
                    ))
                    conn.commit()
                except Exception as e:
                    error_msg = str(e)
                    logging.error(f"Error DB en Hex {hex_id}: {error_msg}")
                    
                    # Detectar pérdida de conexión (FreeTDS 08S01)
                    if '08S01' in error_msg or 'Communication link failure' in error_msg:
                        logging.warning("🔄 Conexión a SQL Server perdida. Intentando reconectar...")
                        
                        # 1. Cerrar la conexión muerta (por si acaso)
                        try:
                            conn.close()
                        except:
                            pass
                        
                        # 2. Pausa breve para dejar que la red se estabilice
                        time.sleep(3) 
                        
                        # 3. Intentar reconectar
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            logging.info("✅ Reconexión a SQL Server exitosa.")
                        except Exception as conn_err:
                            logging.error(f"❌ Fallo crítico al reconectar: {conn_err}")
                        
                        # 4. ROMPER EL BUCLE 'FOR'
                        # Salimos de la iteración de aviones actuales. Evita el spam de logs.
                        # El 'while True' principal volverá a empezar, descargará el JSON fresco 
                        # y retomará el trabajo normalmente.
                        break

            # Latencia del ciclo principal
            time.sleep(1)

    except KeyboardInterrupt: 
        logging.info("🛑 Monitor detenido por el usuario.")
    except Exception as e: 
        logging.error(f"❌ Caída general: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()