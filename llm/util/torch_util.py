import torch

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DEVICE_ID = "0" if torch.cuda.is_available() else None
DEVICE = f"{DEVICE}:{DEVICE_ID}" if DEVICE_ID else DEVICE

def torch_gc():
    if torch.cuda.is_available():
        with torch.cuda.device(DEVICE):
            torch.cuda.empty_cache()
            torch.cuda.ipc_collect()