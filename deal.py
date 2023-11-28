import pandas as pd
import random
import zstandard as zstd
from io import StringIO
import json

def generate_random_tenant(data, num_tenants):
    tenants = data['tntid'].unique().tolist()
    result = random.sample(tenants,num_tenants)
    # print(result)
    return result


def update_operation(row):
    if row['op'] in ['get', 'gets']:
        return 'get'
    else:
        return 'set'


def count_operations(data):
    operation_counts = {}
    keys = {}

    for _, row in data.iterrows():
        tntid = row['tntid']
        op = row['op']

        # Count total set and get operations
        operation_counts.setdefault(tntid, {'set': 0, 'get': 0})
        operation_counts[tntid][op] += 1

        # Count unique keys for each tenant
        keys.setdefault(tntid, set())
        keys[tntid].add(row['key'])

    # Convert sets to lists and count total unique get operations
    total_unique_key_counts = {key: len(value) for key, value in keys.items()}
    # Convert sets to lists
    keys_as_lists = {key: list(value) for key, value in keys.items()}
    # unique_key_counts_serializable = {key: list(value) for key, value in total_unique_key_counts.items()}

    return operation_counts, keys_as_lists, total_unique_key_counts


def create_json_file(operation_counts, total_unique_key_counts, keys, num_tenants, start_time, end_time,iter_id):
    result = {
        'operation_counts': operation_counts,
        'total_unique_key_counts': total_unique_key_counts,
        'keys': keys,
        'num_tenants': num_tenants,
        'start_time': start_time,
        'end_time': end_time
    }

    filename = f'data/trace/operation_counts_tenant{num_tenants}_time{start_time}-{end_time}_iter{iter_id}.json'

    with open(filename, 'w') as json_file:
        json.dump(result, json_file, indent=2)


def select_data(data, start_time, end_time, num_tenants):
    data['ts'] = pd.to_numeric(data['ts'])

    # Filter data based on time duration
    filtered_data = data[(data['ts'] >= start_time) & (data['ts'] <= end_time)]

    selected_tenants = generate_random_tenant(filtered_data,num_tenants)

    # # Randomly select tenants
    # for _ in range(num_tenants):
    #     tenant = generate_random_tenant(filtered_data)
    #     selected_tenants.append(tenant)

    # Filter data based on selected tenants
    final_data = filtered_data[filtered_data['tntid'].isin(selected_tenants)]

    # Update the "operation" column
    final_data['op'] = final_data.apply(update_operation, axis=1)

    return final_data, selected_tenants

# Example usage:
if __name__ == "__main__":
    # Set the time duration (replace 0 and 1 with your desired start and end times)
    start_time = 0
    for end_time in [10,60,900]:
        for num_tenants in [1, 2, 3, 10, 50]:
            for iter_id in [0,1,2]:

                # Replace 'your_data.zst' with the actual path to your zst data file
                with open('data/memcached/cluster01.000.zst', 'rb') as f:
                    dctx = zstd.ZstdDecompressor()
                    compressed_data = f.read()
                    decompressed_data = dctx.decompress(compressed_data)

                # Convert the decompressed data to a DataFrame with modified column names
                column_names = ['ts', 'key', 'key size', 'val_size', 'tntid', 'op', 'ttl']
                data = pd.read_csv(StringIO(decompressed_data.decode('utf-8')), header=None, names=column_names)



                # Call the function to select data based on the specified criteria
                selected_data, selected_tenants = select_data(data, start_time, end_time, num_tenants)

                # Save the selected data to a CSV file with modified file name
                selected_data_filename = f'data/trace/selected_data_tenant{num_tenants}_time{start_time}-{end_time}_iter{iter_id}.csv'
                selected_data.to_csv(selected_data_filename, index=False)

                # Display the selected data with modified column names
                print(selected_data)

                # Count operations and create a JSON file with modified file name
                operation_counts,keys,total_unique_key_counts = count_operations(selected_data)
                create_json_file(operation_counts, total_unique_key_counts, keys, num_tenants, start_time, end_time,iter_id)
                print(f'finished:tenant_num_{num_tenants}-iter_{iter_id}')
