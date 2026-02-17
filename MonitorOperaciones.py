import socket
import pyodbc
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
HOST = 'localhost'
PORT = 30003
TIEMPO_EXPIRACION = 60  # 600 segundos (10 minutos). Si no actualiza en este tiempo, se borra.

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
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=172.16.2.125;'
        'DATABASE=DatosCIRUM;'
        'UID=admin_DB_DatosCIRUM;'
        'PWD=CIRUM/*4dm1n1str4t0r2026'
    )

# Memoria para comparar estados
# Clave: HexIdent | Valor: { 'gnd': bool, 'alt': int, 'estado_actual': str, 'last_seen': timestamp }
memoria_aviones = {}

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
        print(f"🚨 ALERTA ROJA: Emergencia General en {hex_id} (Squawk 7700)")
        return 'EMERGENCIA'
    elif squawk == '7600':
        print(f"📻 ALERTA: Falla de Radio en {hex_id} (Squawk 7600)")
        return 'FALLA_RADIO'
    elif squawk == '7500':
        print(f"☠️ ALERTA CRÍTICA: Secuestro en {hex_id} (Squawk 7500)")
        return 'SECUESTRO'
    return None

def limpiar_inactivos(cursor):
    """Elimina aviones viejos de la DB y de la Memoria Python"""
    # 1. Limpieza en SQL Server
    try:
        # Borramos registros que no se han actualizado en TIEMPO_EXPIRACION segundos
        query_clean = f"""
        DELETE FROM dbo.EstadoAeropuerto 
        WHERE DATEDIFF(second, UltimaActualizacion, GETDATE()) > {TIEMPO_EXPIRACION}
        """
        cursor.execute(query_clean)
        deleted_count = cursor.rowcount
        cursor.commit()
        
        if deleted_count > 0:
            print(f"🧹 Limpieza: Se eliminaron {deleted_count} aviones inactivos de la BD.")

        # 2. Limpieza en Memoria Python
        ahora = time.time()
        # Creamos una lista de las llaves para poder borrar mientras iteramos
        aviones_a_borrar = [k for k, v in memoria_aviones.items() if (ahora - v['last_seen']) > TIEMPO_EXPIRACION]
        
        for hex_id in aviones_a_borrar:
            del memoria_aviones[hex_id]
            
    except Exception as e:
        print(f"⚠️ Error en limpieza: {e}")

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Variable para controlar cada cuánto ejecutamos la limpieza
    ultima_limpieza = time.time()

    try:
        s.connect((HOST, PORT))
        print("🕵️ Monitor de Operaciones...")
        
        buffer_socket = ""
        while True:
            # --- RUTINA DE LIMPIEZA (Cada 60 segundos) ---
            if time.time() - ultima_limpieza > 60:
                limpiar_inactivos(cursor)
                ultima_limpieza = time.time()

            try:
                data = s.recv(4096).decode('utf-8')
            except socket.timeout:
                continue
            except socket.error:
                print("Reconectando...")
                time.sleep(5)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((HOST, PORT))
                continue

            buffer_socket += data
            
            while "\n" in buffer_socket:
                line, buffer_socket = buffer_socket.split("\n", 1)
                fields = line.split(',')
                
                if len(fields) < 22 or fields[0] != 'MSG': continue

                hex_id = fields[4].strip()
                callsign = fields[10].strip() or None
                
                try:
                    alt = int(fields[11]) if fields[11].strip() else None
                    spd = int(fields[12]) if fields[12].strip() else None
                    track = int(fields[13]) if fields[13].strip() else None
                    lat = float(fields[14]) if fields[14].strip() else None
                    lon = float(fields[15]) if fields[15].strip() else None
                    sq = fields[17].strip() or None
                    on_gnd = True if fields[21].strip() != '0' else False
                except ValueError:
                    continue 

                # LÓGICA DE NEGOCIO
                nuevo_estado = None
                pista_actual = None
                
                if sq:
                    estado_emergencia = analizar_squawk(sq, hex_id)
                    if estado_emergencia: nuevo_estado = estado_emergencia

                pista_actual = identificar_pista(lat, lon)

                if not nuevo_estado and hex_id in memoria_aviones:
                    prev = memoria_aviones[hex_id]
                    if prev['gnd'] == False and on_gnd == True:
                        nuevo_estado = 'EN_TIERRA'
                        pista_msg = f" en Pista {pista_actual}" if pista_actual else ""
                        print(f"🛬 ATERRIZAJE: {callsign or hex_id}{pista_msg}")
                    elif prev['gnd'] == True and on_gnd == False and (alt is not None and alt > 200):
                        nuevo_estado = 'EN_VUELO'
                        pista_msg = f" de Pista {pista_actual}" if pista_actual else ""
                        print(f"🛫 DESPEGUE: {callsign or hex_id}{pista_msg}")

                if not nuevo_estado:
                    nuevo_estado = memoria_aviones.get(hex_id, {}).get('estado_actual', 'EN_VUELO')
                    if on_gnd: nuevo_estado = 'EN_TIERRA' 

                # UPSERT BASE DE DATOS
                if lat is not None or nuevo_estado != 'EN_VUELO':
                    query = """
                    MERGE dbo.EstadoAeropuerto AS target
                    USING (SELECT ? AS Hex, ? AS Call) AS source
                    ON (target.HexIdent = source.Hex)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            Callsign = COALESCE(?, target.Callsign),
                            Estado = ?,
                            Latitud = COALESCE(?, target.Latitud),
                            Longitud = COALESCE(?, target.Longitud),
                            Rumbo = COALESCE(?, target.Rumbo),
                            Velocidad = COALESCE(?, target.Velocidad),
                            Altitud = COALESCE(?, target.Altitud),
                            Squawk = COALESCE(?, target.Squawk),
                            PistaProbable = COALESCE(?, target.PistaProbable),
                            HoraAterrizaje = CASE WHEN ? = 'EN_TIERRA' AND target.Estado <> 'EN_TIERRA' THEN GETDATE() ELSE target.HoraAterrizaje END,
                            HoraDespegue = CASE WHEN ? = 'EN_VUELO' AND target.Estado = 'EN_TIERRA' THEN GETDATE() ELSE target.HoraDespegue END,
                            UltimaActualizacion = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (HexIdent, Callsign, Estado, Latitud, Longitud, Rumbo, Velocidad, Altitud, Squawk, PistaProbable, HoraAterrizaje)
                        VALUES (source.Hex, source.Call, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'EN_TIERRA' THEN GETDATE() ELSE NULL END);
                    """
                    cursor.execute(query, (
                        hex_id, callsign,
                        callsign, nuevo_estado, lat, lon, track, spd, alt, sq, pista_actual, nuevo_estado, nuevo_estado,
                        nuevo_estado, lat, lon, track, spd, alt, sq, pista_actual, nuevo_estado
                    ))
                    conn.commit()

                # ACTUALIZAR MEMORIA (Incluyendo last_seen para la limpieza)
                memoria_aviones[hex_id] = {
                    'gnd': on_gnd, 
                    'alt': alt, 
                    'estado_actual': nuevo_estado,
                    'last_seen': time.time() # ¡Importante para la limpieza!
                }

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        time.sleep(5) 

if __name__ == "__main__":
    main()