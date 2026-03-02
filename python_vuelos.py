import pyodbc
import time

# --- CONFIGURACIÓN ---
TIEMPO_MUESTREO = 60  # Segundos entre cada consulta a la vista
TIMEOUT_SESION = 900  # 15 minutos (900s) sin ver el avión para "cerrar" el registro
NOMBRE_VISTA = "dbo.AODB_Radar" # <--- CAMBIA ESTO AL NOMBRE DE TU VISTA

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

def main():
    sesiones_activas = {} # Estructura: {'0D06E5': {'id_db': 15, 'last_seen': 167...}}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🚀 Iniciando Motor de Histórico de Fuerza Bruta...")
    print(f"⏱ Timeout configurado a {TIMEOUT_SESION/60} minutos para separar vuelos del mismo avión.")
    
    try:
        while True:
            ahora = time.time()
            
            # 1. LEER LA VISTA EN VIVO
            try:
                cursor.execute(f"SELECT * FROM {NOMBRE_VISTA}")
                vuelos_vivos = cursor.fetchall()
            except Exception as e:
                print(f"⚠ Error leyendo vista: {e}")
                time.sleep(5)
                # Intentar reconectar
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                except: pass
                continue

            # 2. PROCESAR CADA AVIÓN EN LA VISTA
            for v in vuelos_vivos:
                hex_id = v.HexIdent
                if not hex_id: continue
                
                if hex_id in sesiones_activas:
                    # UPDATE: El avión sigue aquí, actualizamos sus datos en la tabla histórica
                    db_id = sesiones_activas[hex_id]['id_db']
                    query_update = """
                    UPDATE dbo.HistoricoVuelosASA SET
                        Callsign=?, Estado=?, Latitud=?, Longitud=?, Rumbo=?, Velocidad=?, Altitud=?, Squawk=?, PistaProbable=?,
                        HoraAterrizaje=?, HoraDespegue=?, UltimaActualizacion=?, TipoAeronave=?, 
                        asa_Programado=?, asa_Aterrizaje=?, asa_Calzos=?, asa_TipoOperacion=?, asa_EscalaRealICAO=?, 
                        asa_Posicion=?, asa_Terminal=?, asa_EstatusMovOp=?, asa_TipoVuelo=?, asa_TipoAvion=?, asa_Matricula=?,
                        FechaRegistro = GETDATE()
                    WHERE ID = ?
                    """
                    try:
                        cursor.execute(query_update, (
                            v.Callsign, v.Estado, v.Latitud, v.Longitud, v.Rumbo, v.Velocidad, v.Altitud, v.Squawk, v.PistaProbable,
                            v.HoraAterrizaje, v.HoraDespegue, v.UltimaActualizacion, v.TipoAeronave,
                            v.Programado, v.Aterrizaje, v.Calzos, v.asa_TipoOperacion, v.asa_EscalaRealICAO,
                            v.asa_Posicion, v.asa_Terminal, v.asa_EstatusMovOp, v.asa_TipoVuelo, v.asa_TipoAvion, v.asa_Matricula,
                            db_id
                        ))
                        sesiones_activas[hex_id]['last_seen'] = ahora
                    except Exception as e:
                        print(f"Error en UPDATE para {hex_id}: {e}")
                
                else:
                    # INSERT: Es un avión nuevo (o uno que regresó horas después)
                    query_insert = """
                    SET NOCOUNT ON;
                    INSERT INTO dbo.HistoricoVuelosASA (
                        HexIdent, Callsign, Estado, Latitud, Longitud, Rumbo, Velocidad, Altitud, Squawk, PistaProbable,
                        HoraAterrizaje, HoraDespegue, UltimaActualizacion, TipoAeronave, 
                        asa_Programado, asa_Aterrizaje, asa_Calzos, asa_TipoOperacion, asa_EscalaRealICAO, 
                        asa_Posicion, asa_Terminal, asa_EstatusMovOp, asa_TipoVuelo, asa_TipoAvion, asa_Matricula
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    SELECT SCOPE_IDENTITY();
                    """
                    try:
                        cursor.execute(query_insert, (
                            hex_id, v.Callsign, v.Estado, v.Latitud, v.Longitud, v.Rumbo, v.Velocidad, v.Altitud, v.Squawk, v.PistaProbable,
                            v.HoraAterrizaje, v.HoraDespegue, v.UltimaActualizacion, v.TipoAeronave,
                            v.Programado, v.Aterrizaje, v.Calzos, v.asa_TipoOperacion, v.asa_EscalaRealICAO,
                            v.asa_Posicion, v.asa_Terminal, v.asa_EstatusMovOp, v.asa_TipoVuelo, v.asa_TipoAvion, v.asa_Matricula
                        ))
                        nuevo_id = cursor.fetchval()
                        if nuevo_id:
                            sesiones_activas[hex_id] = {'id_db': nuevo_id, 'last_seen': ahora}
                            print(f"🆕 Nuevo Registro Histórico Creado: {hex_id} (Vuelo: {v.Callsign})")
                    except Exception as e:
                        print(f"Error en INSERT para {hex_id}: {e}")

            conn.commit()

            # 3. LIMPIEZA DE SESIONES CERRADAS (El avión se fue)
            para_borrar = []
            for hx, info in sesiones_activas.items():
                if (ahora - info['last_seen']) > TIMEOUT_SESION:
                    para_borrar.append(hx)
            
            for hx in para_borrar:
                print(f"🔒 Sesión cerrada y guardada para {hx}. Ya no está en el radar.")
                del sesiones_activas[hx]

            time.sleep(TIEMPO_MUESTREO)

    except KeyboardInterrupt:
        print("\n🛑 Apagando recolector de históricos...")
        conn.close()

if __name__ == "__main__":
    main()
