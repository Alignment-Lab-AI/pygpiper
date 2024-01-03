import yaml
import os
import multiprocessing
from datasets import load_dataset
import subprocess
import json
import shutil
from tqdm import tqdm

def read_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def convert_to_yaml(data_tuple):
    data, output_dir, row_number = data_tuple
    filename = os.path.join(output_dir, f'{row_number}.yaml')
    with open(filename, 'w') as file:
        yaml.dump(data, file)

def clean_file(file_path):
    subprocess.run(['python', 'src/clean.py', file_path])

def convert_yaml_to_jsonl(input_dir, output_file):
    jsonl_data = []
    for yaml_file in os.listdir(input_dir):
        if yaml_file.endswith('.yaml'):
            with open(os.path.join(input_dir, yaml_file), 'r') as file:
                data = yaml.safe_load(file)
                jsonl_data.append(json.dumps(data))
    
    with open(output_file, 'w') as file:
        file.write('\n'.join(jsonl_data))

def zip_directory(source_dir, output_zip):
    shutil.make_archive(output_zip, 'zip', source_dir)

def make_hashable(obj):
    if isinstance(obj, list):
        return tuple(make_hashable(x) for x in obj)
    elif isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    else:
        return obj

import json

def process_data(config):
    input_file = config['input_file']
    input_path, input_filename = os.path.split(input_file)
    input_extension = input_filename.split('.')[-1]

    if input_extension.lower() == "jsonl":
        # Load local JSONL file
        dataset = load_dataset('json', data_files=input_file)
        data = dataset['train']
    else:
        # Load other types of datasets (including those from Hugging Face Hub)
        dataset = load_dataset(input_file)
        data = dataset['train']

    # Deduplicate the dataset
    unique_rows = set()
    deduplicated_data = []
    for row in data:
        row_json = json.dumps(row, sort_keys=True)
        if row_json not in unique_rows:
            unique_rows.add(row_json)
            deduplicated_data.append(row)

    base_filename, _ = os.path.splitext(input_filename)
    base_dir = os.path.join(input_path, base_filename)

    unclean_dir = os.path.join(base_dir, 'unclean')
    clean_dir = os.path.join(base_dir, 'partial')
    os.makedirs(unclean_dir, exist_ok=True)
    os.makedirs(clean_dir, exist_ok=True)

    # Convert to YAML with progress bar and multiprocessing
    print("Converting to YAML...")
    args = [(row, unclean_dir, i) for i, row in enumerate(deduplicated_data)]
    with multiprocessing.Pool() as pool:
        list(tqdm(pool.imap_unordered(convert_to_yaml, args), total=len(args), desc="Converting", unit="file"))

    # Cleaning with progress bar and multiprocessing
    print("Cleaning files...")
    files = [os.path.join(unclean_dir, f) for f in os.listdir(unclean_dir) if f.endswith('.yaml')]
    with multiprocessing.Pool() as pool:
        list(tqdm(pool.imap_unordered(clean_file, files), total=len(files), desc="Cleaning", unit="file"))

    # Running token counting
    print("Running token counting...")
    subprocess.run(['python', 'src/tokencounter.py', clean_dir])

    # Convert to JSONL with progress bar
    print("Converting to JSONL...")
    final_jsonl = os.path.join(os.getcwd(), f'/aug/{base_filename}_final.jsonl')
    convert_yaml_to_jsonl(clean_dir, final_jsonl)

    # Zip the directory
    print("Zipping the directory...")
    archive_dir = os.path.join(os.getcwd(), 'archive')
    os.makedirs(archive_dir, exist_ok=True)
    zip_directory(base_dir, os.path.join(archive_dir, base_filename))

    # Extract the final folder from the archive into /aug/ as a final step
    print("Extracting the final folder...")
    shutil.unpack_archive(os.path.join(archive_dir, base_filename + '.zip'), os.path.join(os.getcwd(), 'aug'))

    # Clean up uncompressed directory
    print("Cleaning up uncompressed directory...")
    shutil.rmtree(base_dir)

if __name__ == "__main__":
    config = read_config('config.yml')
    process_data(config)

