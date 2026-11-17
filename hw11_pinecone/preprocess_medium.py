"""
Step 3: Preprocess Medium data and save it as a file.

- Loads the Kaggle Medium articles CSV (arnabchaki/medium-articles-dataset)
- Creates a new column "metadata" = {'title': title + " " + subtitle}
  using pandas apply with axis=1 (needs two columns from the same row)
- Saves the result to a file
"""
import glob
import sys

import pandas as pd

# Find the CSV that `kaggle datasets download ... --unzip` extracted
csv_files = glob.glob("*.csv")
csv_files = [f for f in csv_files if not f.startswith("medium_preprocessed")]
if not csv_files:
    sys.exit("No CSV found. Run this from the folder where you downloaded the dataset.")
input_file = csv_files[0]
print(f"Reading: {input_file}")

df = pd.read_csv(input_file)
print(f"Rows: {len(df)}")
print("Columns:", list(df.columns))

# Some articles have no subtitle (NaN). NaN + string raises a TypeError,
# so replace missing values with empty strings first.
df["title"] = df["title"].fillna("").astype(str)
df["subtitle"] = df["subtitle"].fillna("").astype(str)

# Create "metadata" with pandas apply (axis=1 = row by row)
df["metadata"] = df.apply(
    lambda row: {"title": row["title"] + " " + row["subtitle"]},
    axis=1
)

print("\nSample:")
print(df[["title", "subtitle", "metadata"]].head())

# Save as JSON Lines so the metadata dict is kept as a real object
output_file = "medium_preprocessed.jsonl"
df.to_json(output_file, orient="records", lines=True)

# Also save a CSV copy for easy viewing in Excel/Numbers
df.to_csv("medium_preprocessed.csv", index=False)

print(f"\nSaved {len(df)} rows to {output_file} and medium_preprocessed.csv")
