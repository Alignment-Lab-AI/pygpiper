import os
import multiprocessing
import yaml
from tqdm import tqdm
from datasets import load_dataset


def read_config(config_file: str) -> dict:
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)


def convert_to_yaml(data_tuple) -> None:
    data, output_dir, row_number = data_tuple
    filename = os.path.join(output_dir, f'{row_number}.yaml')
    with open(filename, 'w') as file:
        yaml.dump(data, file)


def clean_file(file_path: str) -> None:
    from .clean import clean_data_main

    clean_data_main(file_path)


def token_count(file_path: str) -> None:
    from .tokencounter import tokencounter

    tokencounter(file_path)


def process_data(config_path: str) -> None:
    config = read_config(config_path)
    dataset_path = config['input_file']
    base_dir = os.path.dirname(dataset_path)
    dataset_dir = os.path.basename(dataset_path)
    temp_dataset_path = os.path.join(base_dir, 'temp_' + dataset_dir)

    unclean_dir = os.path.join(base_dir, dataset_dir, 'unclean')
    partial_dir = os.path.join(base_dir, dataset_dir, 'partial')
    final_dir = os.path.join(base_dir, dataset_dir, 'final')

    os.makedirs(unclean_dir, exist_ok=True)
    os.makedirs(partial_dir, exist_ok=True)
    os.makedirs(final_dir, exist_ok=True)

    # Rename the directory and load the dataset
    if os.path.isdir(dataset_path):
        os.rename(dataset_path, temp_dataset_path)
        dataset = load_dataset(dataset_path)
        os.rename(temp_dataset_path, dataset_path)
    else:
        dataset = load_dataset(dataset_path)

    data = dataset['train']

    # Convert to YAML step
    if not os.listdir(unclean_dir):
        print('Converting to YAML...')
        args = [(row, unclean_dir, i) for i, row in enumerate(data)]
        with multiprocessing.Pool() as pool:
            list(
                tqdm(
                    pool.imap_unordered(convert_to_yaml, args),
                    total=len(args),
                    desc='Converting',
                    unit='file',
                )
            )

    # Clean files step
    if not os.listdir(partial_dir):
        print('Cleaning files...')
        unclean_files = [
            os.path.join(unclean_dir, f) for f in os.listdir(unclean_dir) if f.endswith('.yaml')
        ]
        for file in tqdm(unclean_files, desc='Cleaning', unit='file'):
            clean_file(file)

    # Token counting step
    if not os.listdir(final_dir):
        print('Running token counting...')
        partial_files = [os.path.join(partial_dir)]
        for file in tqdm(partial_files, desc='Token Counting', unit='file'):
            token_count(file)


if __name__ == '__main__':
    process_data('config.yml')
