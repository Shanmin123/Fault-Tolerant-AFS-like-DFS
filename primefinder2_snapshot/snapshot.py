import pickle
import os
def save_snapshot(file_name, data):
    os.makedirs("snapshots", exist_ok=True)
    with open(f"snapshots/{file_name}.pkl", "wb") as f:
        pickle.dump(data, f)

def load_snapshot(file_name):
    path = f"snapshots/{file_name}.pkl"
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    return None