import os
import sys
import torch

from .utils import load_config, read_yaml, write_yaml


def load_tokenizer_from_config(config: dict) -> torch.nn.Module:
    tokenizer = None
    tokenizer_cmd = config['tokenizer_info']['tokenizer']
    exec(tokenizer_cmd, globals())
    return tokenizer  # type: ignore


def count_tokens(data, tokenizer, keys_to_count):
    total_token_count = 0
    for key in keys_to_count:
        if key in data:
            text = data[key]
            if isinstance(text, str) and text:
                total_token_count += len(tokenizer.encode(text, add_special_tokens=False))
    return total_token_count


def split_conversations(data, max_tokens, min_tokens, tokenizer, static_keys, keys_to_count):
    static_token_count = count_tokens(
        {key: data[key] for key in static_keys if key in data}, tokenizer, static_keys
    )

    current_conv = []
    current_token_count = static_token_count
    split_index = 1
    for turn in data['conversation']:
        turn_token_count = len(tokenizer.encode(turn['value'], add_special_tokens=False))

        if current_token_count + turn_token_count > max_tokens:
            print(f'Splitting at token count: {current_token_count}. Split index: {split_index}')
            yield current_conv
            current_conv = [turn]
            current_token_count = static_token_count + turn_token_count
            split_index += 1
        else:
            current_conv.append(turn)
            current_token_count += turn_token_count

    if current_conv and current_token_count >= min_tokens:
        print(
            f'Yielding final part at token count: {current_token_count}. Split index: {split_index}'
        )
        yield current_conv


def process_files(input_dir, output_dir, config, batch_size=1000):
    tokenizer = load_tokenizer_from_config(config)
    max_tokens = config['tokenizer_info']['max_tokens']
    min_tokens = 200
    static_keys = config['tokenizer_info']['static_keys']
    keys_to_count = config['tokenizer_info']['keys_to_count']

    all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.yaml')]
    for file_name in all_files:
        data = read_yaml(file_name)
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        print(f'Processing file: {file_name}')

        for i, conv_part in enumerate(
            split_conversations(data, max_tokens, min_tokens, tokenizer, static_keys, keys_to_count)
        ):
            new_data = {key: data[key] for key in static_keys}
            new_data['conversation'] = conv_part
            output_file_name = f'{base_name}_{i}.yaml'
            write_yaml(new_data, os.path.join(output_dir, output_file_name))


def tokencounter() -> None:
    config = load_config('config.yml')
    input_dir = config['tokenizer_info']['input_dir']
    base_dir = os.path.dirname(input_dir)
    output_dir = os.path.join(base_dir, 'final')
    os.makedirs(output_dir, exist_ok=True)

    process_files(input_dir, output_dir, config)


if __name__ == '__main__':
    tokencounter()
