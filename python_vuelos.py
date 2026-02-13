import socket
import pyodbc
import time

# --- CONFIGURACIÓN ---
HOST = 'localhost'
PORT = 30003
TIMEOUT_SESION = 1200  # 20 minutos

# --- CONEXIÓN BASE DE DATOS ---
def get_db_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=172.16.2.125;'
        'DATABASE=DatosCIRUM;'
        'UID=admin_DB_DatosCIRUM;'
        'PWD=CIRUM/*4dm1n1str4t0r2026'
    )

# --- MEMORIA DE SESIONES ---
sesiones_activas = {}

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"📡 Conectando a {HOST}:{PORT}...")
    
    try:
        s.connect((HOST, PORT))
        print(f"✅ Conectado. Modo Sesión Activa (Timeout: {TIMEOUT_SESION}s).")
        
        buffer_socket = ""
        
        while True:
            try:
                data = s.recv(4096).decode('utf-8')
                buffer_socket += data
                
                while "\n" in buffer_socket:
                    line, buffer_socket = buffer_socket.split("\n", 1)
                    fields = line.split(',')
                    
                    if len(fields) < 22 or fields[0] != 'MSG':
                        continue

                    hex_id = fields[4].strip()
                    
                    datos_msg = {
                        'Callsign': fields[10].strip() or None,
                        'Alt': int(fields[11]) if fields[11].strip() else None,
                        'Spd': int(fields[12]) if fields[12].strip() else None,
                        'Track': int(fields[13]) if fields[13].strip() else None,
                        'Lat': float(fields[14]) if fields[14].strip() else None,
                        'Lon': float(fields[15]) if fields[15].strip() else None,
                        'VR': int(fields[16]) if fields[16].strip() else None,
                        'Sq': fields[17].strip() or None,
                        'Gnd': 1 if fields[21].strip() != '0' else 0
                    }

                    ahora = time.time()

                    # 1. Limpiar sesiones expiradas
                    if hex_id in sesiones_activas:
                        tiempo_inactivo = ahora - sesiones_activas[hex_id]['last_seen']
                        if tiempo_inactivo > TIMEOUT_SESION:
                            print(f"⏱ Sesión expirada para {hex_id}. Reiniciando.")
                            del sesiones_activas[hex_id]

                    # 2. Lógica de Guardado
                    if hex_id in sesiones_activas:
                        # UPDATE (Ya existe)
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
                            RegistroFecha = GETDATE()
                        WHERE ID = ?
                        """
                        cursor.execute(query_update, (
                            datos_msg['Callsign'], datos_msg['Alt'], datos_msg['Spd'], datos_msg['Track'],
                            datos_msg['Lat'], datos_msg['Lon'], datos_msg['VR'], datos_msg['Sq'],
                            datos_msg['Gnd'], db_id
                        ))
                        conn.commit()
                        sesiones_activas[hex_id]['last_seen'] = ahora

                    else:
                        # INSERT (Nuevo Vuelo) - AQUÍ ESTABA EL ERROR
                        if datos_msg['Lat'] is not None:
                            # Agregamos 'SET NOCOUNT ON;' al principio
                            query_insert = """
                            SET NOCOUNT ON; 
                            INSERT INTO dbo.RegVuelosDUMP 
                            (HexIdent, Callsign, Altitude, GroundSpeed, Track, Latitude, Longitude, VerticalRate, Squawk, IsOnGround, RegistroFecha)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                            SELECT SCOPE_IDENTITY();
                            """
                            
                            cursor.execute(query_insert, (
                                hex_id, datos_msg['Callsign'], datos_msg['Alt'], datos_msg['Spd'], datos_msg['Track'],
                                datos_msg['Lat'], datos_msg['Lon'], datos_msg['VR'], datos_msg['Sq'], datos_msg['Gnd']
                            ))
                            
                            # Ahora sí funcionará fetchval()
                            nuevo_id = cursor.fetchval()
                            conn.commit()
                            
                            if nuevo_id:
                                sesiones_activas[hex_id] = {'db_id': nuevo_id, 'last_seen': ahora}
                                print(f"🆕 Nuevo Vuelo: {hex_id} (ID: {nuevo_id})")

            except socket.error:
                print("⚠ Conexión perdida. Reintentando...")
                time.sleep(5)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((HOST, PORT))

    except KeyboardInterrupt:
        print("\n🛑 Cerrando...")
        conn.close()

if __name__ == "__main__":
    main()