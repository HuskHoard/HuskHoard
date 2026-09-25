# this script is supporting material for a proxy workflow demo https://youtu.be/izFoB7Sfhnw Include a file "husk_sidecar.sock" in your huskhoard
# folder along with this sidecar.py script. place this line in the husk_config.toml: 
# sidecar_socket_path = "husk_sidecar.sock" 
# You must start this process before you run the daemon or you will get an error. 


import socket, json, os, subprocess

SOCKET_PATH = "husk_sidecar.sock"
if os.path.exists(SOCKET_PATH): 
    os.remove(SOCKET_PATH)

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(SOCKET_PATH)
server.listen(1)
print(f"[*] Sidecar listening on {SOCKET_PATH}...")

while True:
    conn, _ = server.accept()
    data = conn.recv(4096).decode().strip()
    if not data: 
        conn.close()
        continue
    
    try:
        req = json.loads(data)
        action = req.get("action")
        event = req.get("event")
        
        if action == "PRE_ARCHIVE":
            src = req["file_path"]
            # Strip the existing extension before adding .mp4
            name_without_ext = os.path.splitext(os.path.basename(src))[0]
            proxy_dir = "hot_tier/Proxies"
            proxy_path = os.path.join(proxy_dir, f"proxy_{name_without_ext}.mp4")
            
            os.makedirs(proxy_dir, exist_ok=True)
            
            # Check if Proxy exists to avoid re-encoding the same files
            if os.path.exists(proxy_path):
                print(f"[Sidecar] Proxy already exists, skipping: {proxy_path}")
                response = json.dumps({"proxy_generated": proxy_path, "status": "OK"}) + "\n"
            else:
                print(f"\n[Sidecar] Generating low-res proxy for {src} -> {proxy_path}")
                cmd = ["ffmpeg", "-y", "-i", src, "-vf", "scale=-2:480", "-c:v", "libx264", "-crf", "28", "-preset", "veryfast", "-c:a", "aac", proxy_path]
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"[Sidecar] Success! Created {proxy_path}")
                    response = json.dumps({"proxy_generated": proxy_path, "status": "OK"}) + "\n"
                else:
                    print(f"[Sidecar] FFMPEG ERROR:\n{result.stderr}")
                    response = json.dumps({"proxy_generated": None, "status": "ERROR", "error": "FFmpeg failed"}) + "\n"
                    
            conn.sendall(response.encode())
            
        elif action == "WAKE_VOLUME":
            device = req.get("device_path")
            print(f"[Sidecar] Hardware check: Volume {device} is active.")
            # Reply READY so Husk doesn't drop the connection
            conn.sendall(b"READY\n")
            
        elif event:
            # Silently accept (and ignore) one-way Telemetry broadcasts from Husk
            pass
            
    except json.JSONDecodeError:
        print("[Sidecar] Received invalid JSON")
    except Exception as e:
        print(f"[Sidecar] Unexpected error: {e}")
        
    finally:
        conn.close()
