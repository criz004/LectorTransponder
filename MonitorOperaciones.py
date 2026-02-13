import socket
import pyodbc
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
HOST = 'localhost'
PORT = 30003

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
memoria_aviones = {}

def analizar_squawk(squawk, hex_id):
    """Retorna un estado especial si el código es de emergencia"""
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

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((HOST, PORT))
        print("🕵️ Monitor de Operaciones ACTIVO (Mapa + Alertas)...")
        
        buffer_socket = ""
        while True:
            data = s.recv(4096).decode('utf-8')
            buffer_socket += data
            
            while "\n" in buffer_socket:
                line, buffer_socket = buffer_socket.split("\n", 1)
                fields = line.split(',')
                
                if len(fields) < 22 or fields[0] != 'MSG': continue

                # Extraer datos crudos
                hex_id = fields[4].strip()
                callsign = fields[10].strip() or None
                
                # Datos numéricos seguros
                try:
                    alt = int(fields[11]) if fields[11].strip() else None
                    spd = int(fields[12]) if fields[12].strip() else None
                    track = int(fields[13]) if fields[13].strip() else None
                    lat = float(fields[14]) if fields[14].strip() else None
                    lon = float(fields[15]) if fields[15].strip() else None
                    sq = fields[17].strip() or None
                    on_gnd = True if fields[21].strip() != '0' else False
                except ValueError:
                    continue # Si un dato viene corrupto, saltamos

                # 1. Determinación de Estado (Lógica de Negocio)
                nuevo_estado = None
                
                # Chequeo de Emergencia Prioritario
                if sq:
                    estado_emergencia = analizar_squawk(sq, hex_id)
                    if estado_emergencia:
                        nuevo_estado = estado_emergencia

                # Si no es emergencia, calculamos estado operativo
                if not nuevo_estado and hex_id in memoria_aviones:
                    prev = memoria_aviones[hex_id]
                    
                    # Aterrizaje
                    if prev['gnd'] == False and on_gnd == True:
                        nuevo_estado = 'EN_TIERRA'
                        print(f"🛬 ATERRIZAJE: {callsign or hex_id}")
                    
                    # Despegue
                    elif prev['gnd'] == True and on_gnd == False and (alt is not None and alt > 200):
                        nuevo_estado = 'EN_VUELO'
                        print(f"🛫 DESPEGUE: {callsign or hex_id}")
                    
                    # Movimiento en tierra (Taxeo)
                    elif on_gnd == True and spd is not None and spd > 1 and prev['estado_actual'] != 'TAXEO':
                        # Opcional: Si quieres distinguir entre 'Parado' y 'Taxeo'
                        pass 

                # Si no cambió, mantenemos el anterior o default
                if not nuevo_estado:
                    nuevo_estado = memoria_aviones.get(hex_id, {}).get('estado_actual', 'EN_VUELO')
                    if on_gnd: nuevo_estado = 'EN_TIERRA' # Corrección simple

                # 2. Actualización en Base de Datos (Upsert Constante para el Mapa)
                # Solo actualizamos si tenemos coordenadas (para el mapa) o si cambió el estado
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
                            HoraAterrizaje = CASE WHEN ? = 'EN_TIERRA' AND target.Estado <> 'EN_TIERRA' THEN GETDATE() ELSE target.HoraAterrizaje END,
                            HoraDespegue = CASE WHEN ? = 'EN_VUELO' AND target.Estado = 'EN_TIERRA' THEN GETDATE() ELSE target.HoraDespegue END,
                            UltimaActualizacion = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (HexIdent, Callsign, Estado, Latitud, Longitud, Rumbo, Velocidad, Altitud, Squawk, HoraAterrizaje)
                        VALUES (source.Hex, source.Call, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'EN_TIERRA' THEN GETDATE() ELSE NULL END);
                    """
                    
                    cursor.execute(query, (
                        # Source
                        hex_id, callsign,
                        # Update Values
                        callsign, nuevo_estado, lat, lon, track, spd, alt, sq, nuevo_estado, nuevo_estado,
                        # Insert Values
                        nuevo_estado, lat, lon, track, spd, alt, sq, nuevo_estado
                    ))
                    conn.commit()

                # Actualizar memoria
                memoria_aviones[hex_id] = {
                    'gnd': on_gnd, 
                    'alt': alt, 
                    'estado_actual': nuevo_estado
                }

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(5) # Evitar bucle infinito de errores rapido

if __name__ == "__main__":
    main()