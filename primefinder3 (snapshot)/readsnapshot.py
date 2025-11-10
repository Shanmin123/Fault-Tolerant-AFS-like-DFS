import pickle

with open("snapshots/snapshot_1.pkl", "rb") as f:
    data = pickle.load(f)
print(data)

with open("snapshots/snapshot_2.pkl", "rb") as f:
    data = pickle.load(f)

print(data)

with open("snapshots/snapshot_3.pkl", "rb") as f:
    data = pickle.load(f)


print(data)