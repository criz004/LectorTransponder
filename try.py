import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 30003))

with open("dump1090_output.txt", "a", encoding="utf-8") as f:
    while True:
        data = s.recv(1024)
        if not data:
            break

        text = data.decode("utf-8", errors="ignore")
        print(text, end="")      # optional: still show it
        f.write(text)
        f.flush()                # ensures data is written immediately
