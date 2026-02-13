import subprocess
import time
import urllib.request
import modal

APP_NAME = "android-risk-vllm"
MODEL_ID = "mistralai/Mistral-7B-Instruct-v0.2"
API_KEY = "token-abc123"

app = modal.App(APP_NAME)

image = modal.Image.debian_slim().pip_install("vllm", "huggingface_hub")

volume = modal.Volume.from_name("vllm-model-cache", create_if_missing=True)

@app.function(
    image=image,
    gpu="A10G",
    volumes={"/models": volume},
    timeout=60 * 30,
)
@modal.web_server(8000)
def serve():
    cmd = [
        "vllm", "serve", MODEL_ID,
        "--host", "0.0.0.0",
        "--port", "8000",
        "--download-dir", "/models",
        "--api-key", API_KEY,
    ]
    subprocess.Popen(cmd)

    # Wait until vLLM is actually responding
    for _ in range(180):  # up to ~3 minutes
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:8000/v1/models",
                headers={"Authorization": f"Bearer {API_KEY}"}
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("vLLM did not become ready in time")

    while True:
        time.sleep(60)