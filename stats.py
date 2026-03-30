import pandas as pd

df = pd.read_csv("stats.csv")
df["cpu"] = df["cpu"].str.rstrip("%").astype(float)

# Average CPU per container
print(df.groupby("container")["cpu"].mean().sort_values(ascending=False))