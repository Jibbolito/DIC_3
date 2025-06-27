import os
import json
import math

# Path to the JSON file
input_path = os.path.join(os.path.dirname(__file__), 'data', 'reviews_devset.json')

# Load the JSON data (one JSON object per line)
with open(input_path, 'r') as f:
    data = [json.loads(line) for line in f if line.strip()]

# Number of batches
num_batches = 10
batch_size = math.ceil(len(data) / num_batches)

# Output directory for batches
output_dir = os.path.join(os.path.dirname(__file__), 'data', 'batches')
os.makedirs(output_dir, exist_ok=True)

# Split and save batches
for i in range(num_batches):
    start = i * batch_size
    end = start + batch_size
    batch = data[start:end]
    if not batch:
        break
    batch_path = os.path.join(output_dir, f'batch_{i+1}.json')
    with open(batch_path, 'w') as f:
        for obj in batch:
            json.dump(obj, f)
            f.write('\n')

print(f"Split into {i+1} batches in {output_dir}")