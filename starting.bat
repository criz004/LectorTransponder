:: ---------------------------------------------------------------------------

:: 1. Ir a la carpeta del proyecto (CRÍTICO)
cd /d "C:\Users\ccoaicm\Documents\TransXD"

:: 2. Despertar WSL y mantenerlo VIVO (El cambio importante)
:: Usamos 'dbus-launch true'. Esto inicia un proceso en Linux que se queda
:: esperando en el fondo, lo que impide que Windows apague la máquina virtual.
wsl -d Ubuntu --exec dbus-launch true

:: 3. CONECTAR EL USB (Si no funciona el auto-attach)
:: Intentamos conectar. Si ya está conectado, esto dará error pero no importa.
usbipd attach --wsl --busid 1-2

:: 4. Espera de seguridad
timeout /t 10 /nobreak > nul

:: 5. Iniciar el Guardado de Datos (Python)
python python_vuelos.py