import pandas as pd
import sys
readFile = sys.argv[1]
writeFile = sys.argv[2]

print(f"Arguments: {sys.argv}")

print(f"Reading data in from {readFile}")
df = pd.read_csv(readFile)
print(df.head(3))
print("------------------------------------")
print("Renaming Columns:")
print("Sorting in ascending order or happiness scores")
df.rename(columns={"country": "Country", "score": "HappinessScore", 'income':'Income'}, inplace=True)
df.sort_values('HappinessScore',inplace=True)
print(df.head(3))

print("------------------------------------")
print(f"Writing altered data to {writeFile}")
df.to_csv(writeFile)

print(f"Pipeline successfully piped")