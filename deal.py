import pandas as pd
import random
import zstandard as zstd
from io import StringIO
import json

def generate_random_tenant(data):
    tenants = data['tntid'].unique()
    return random.choice(tenants)

def update_operation(row):
    if row['op'] in ['get', 'gets']:
        return 'get'
    else:
        return 'set'

def count_operations(data):
    operation_counts = {}
    unique_get_counts = {}

    for _, row in data.iterrows():
        tntid = row['tntid']
        op = row['op']

        # Count total set and get operations
        operation_counts.setdefault(tntid, {'set': 0, 'get': 0})
        operation_counts[tntid][op] += 1

        # Count unique get operations
        if op == 'get':
            unique_get_counts.setdefault(tntid, set())
            unique_get_counts[tntid].add(row['key'])

    # Convert sets to lists and count total unique get operations
    total_unique_get_counts = {key: len(value) for key, value in unique_get_counts.items()}
    unique_get_counts_serializable = {key: list(value) for key, value in unique_get_counts.items()}

    return operation_counts, total_unique_get_counts, unique_get_counts_serializable

def create_json_file(operation_counts, total_unique_get_counts, unique_get_counts, tenant_num, start_time, end_time):
    result = {
        'operation_counts': operation_counts,
        'total_unique_get_counts': total_unique_get_counts,
        'unique_get_counts': unique_get_counts
    }

    filename = f'data/memcached/trace/operation_counts_tenant{tenant_num}_time{start_time}-{end_time}.json'

    with open(filename, 'w') as json_file:
        json.dump(result, json_file, indent=2)

def select_data(data, start_time, end_time, num_tenants):
    data['ts'] = pd.to_numeric(data['ts'])
    
    # Filter data based on time duration
    filtered_data = data[(data['ts'] >= start_time) & (data['ts'] <= end_time)]
    
    selected_tenants = []
    
    # Randomly select tenants
    for _ in range(num_tenants):
        tenant = generate_random_tenant(filtered_data)
        selected_tenants.append(tenant)
    
    # Filter data based on selected tenants
    final_data = filtered_data[filtered_data['tntid'].isin(selected_tenants)]

    # Update the "operation" column
    final_data['op'] = final_data.apply(update_operation, axis=1)
    
    return final_data, selected_tenants

# Example usage:
if __name__ == "__main__":
    # Replace 'your_data.zst' with the actual path to your zst data file
    with open('data/memcached/cluster01.000.zst', 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        compressed_data = f.read()
        decompressed_data = dctx.decompress(compressed_data)

    # Convert the decompressed data to a DataFrame with modified column names
    column_names = ['ts', 'key', 'key size', 'val_size', 'tntid', 'op', 'ttl']
    data = pd.read_csv(StringIO(decompressed_data.decode('utf-8')), header=None, names=column_names)

    # Set the time duration (replace 0 and 1 with your desired start and end times)
    start_time = 0
    end_time = 1

    # Set the number of tenants to randomly select
    num_tenants = 5

    # Call the function to select data based on the specified criteria
    selected_data, selected_tenants = select_data(data, start_time, end_time, num_tenants)

    # Save the selected data to a CSV file with modified file name
    selected_data_filename = f'data/memcached/trace/selected_data_tenant{num_tenants}_time{start_time}-{end_time}.csv'
    selected_data.to_csv(selected_data_filename, index=False)

    # Display the selected data with modified column names
    print(selected_data)

    # Count operations and create a JSON file with modified file name
    operation_counts, total_unique_get_counts, unique_get_counts = count_operations(selected_data)
    create_json_file(operation_counts, total_unique_get_counts, unique_get_counts, num_tenants, start_time, end_time)
