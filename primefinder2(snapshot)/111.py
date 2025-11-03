import pickle

with open("snapshots/coordinator_snapshot.pkl", "rb") as f:
    data = pickle.load(f)
print(data)

with open("snapshots/coordinator_snapshot.pkl", "rb") as f:
    data = pickle.load(f)

print(data)
