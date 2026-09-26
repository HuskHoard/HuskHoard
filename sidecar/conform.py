import subprocess

HUSK_GATEWAY = "http://192.168.0.90:8080/stream"

sequence = [
    {"file": "chunk1.mp4", "start": "00:01:00", "duration": "20", "out_name": "scene1_clipA.mp4"},
    {"file": "chunk2.mp4", "start": "00:02:00", "duration": "20", "out_name": "scene1_clipB.mp4"}
]

for clip in sequence:
    url = f"{HUSK_GATEWAY}/{clip['file']}"
    cmd = [
        "ffmpeg", "-y",
        "-ss", clip["start"],         # Seek over HTTP using Husk byte-ranges
        "-i", url,
        "-t", clip["duration"],       # Grab 20 seconds
        "-c", "copy",                 # Fast zero-loss stream copy
        clip["out_name"]
    ]
    subprocess.run(cmd, check=True)
