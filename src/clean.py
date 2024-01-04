import yaml
import sys
import os
import re
import json

def load_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def apply_exact_replacements(data, replacements, recursive):
    if recursive:
        changed = True
        while changed:
            changed = False
            for old, new in replacements:
                new_data = data.replace(old, new)
                if new_data != data:
                    changed = True
                    data = new_data
    else:
        for old, new in replacements:
            data = data.replace(old, new)
    return data

def trim_whitespace(data):
    return data.strip()

def strip_unicode(data, codec):
    try:
        # Attempt to encode and decode back using the specified codec, ignoring errors
        return data.encode(codec, 'ignore').decode(codec)
    except UnicodeError:
        # In case of a codec error, return the original data
        return data

def regex_replace(data, replacements):
    for pattern, repl in replacements:
        compiled_pattern = re.compile(pattern)
        data = compiled_pattern.sub(repl, data)
    return data

def drop_empty_messages(data, key):
    return [item for item in data if item.get(key, '').strip()]

from itertools import groupby

def remove_consecutive_duplicate_messages(conversation):
    """
    Remove consecutive duplicates from the conversation based on 'message'.
    Only duplicates that are directly following each other are removed.
    """
    # Group by 'message' and take the first entry from each group
    return [next(group) for key, group in groupby(conversation, lambda x: x['message'])]
    
def replace_bot_name(data, bot_name):
    bot_name_full = bot_name.strip()
    bot_name_first_word = bot_name_full.split()[0] if bot_name_full else ''
    for message in data:
        if bot_name_full in message['value']:
            message['value'] = message['value'].replace(bot_name_full, '{{char}}')
        if bot_name_first_word in message['value']:
            message['value'] = message['value'].replace(bot_name_first_word, '{{char}}')
    return data
        

def _clean_data(input_file, config, output_dir, file_index):
    with open(input_file, 'r') as file:
        data = yaml.safe_load(file)

    # Apply cleaning steps
    for step in config['step']:
        step_type = step.get('type', '')
        if step_type == 'ExactReplace':
            for item in data:
                data[item] = apply_exact_replacements(data[item], step['config']['replacements'], step['config'].get('recursive', False))
        elif step_type == 'Trim':
            for item in data:
                data[item] = trim_whitespace(data[item])
        elif step_type == 'Encoding':
            for item in data:
                data[item] = strip_unicode(data[item], step['config']['codec'])
        elif step_type == 'RegexReplace':
            for item in data:
                data[item] = regex_replace(data[item], step['config']['replacements'])
        elif step_type == 'FullMatch':
            data[step['config']['key']] = drop_empty_messages(data[step['config']['key']], step['config']['key'])
            
def replace_bot_name_in_message(message, bot_name):
    bot_name_full = bot_name.strip()
    bot_name_first_word = bot_name_full.split()[0] if bot_name_full else ''
    if bot_name_full in message or bot_name_first_word in message:
        message = message.replace(bot_name_full, '{{char}}')
        message = message.replace(bot_name_first_word, '{{char}}')
    return message

def remove_consecutive_duplicate_values(conversation):
    """
    Remove consecutive duplicates from the conversation based on 'value'.
    Only duplicates that are directly following each other are removed.
    """
    return [next(group) for key, group in groupby(conversation, lambda x: x['value'])]

def clean_data(input_file, config, output_dir, file_index):
    with open(input_file, 'r') as file:
        data = yaml.safe_load(file)

    bot_name = data.get("bot_name", "")            

    # Remove consecutive identical entries
    updated_conversation = []
    for message in data.get('conversation', []):
        # Replace bot name in message
        updated_message = replace_bot_name_in_message(message.get('message', ''), bot_name)

        # Rename keys and update values
        new_message = {
            'from': '{{char}}' if not message.get('is_human', True) else '{{user}}',
            'value': updated_message
        }
        updated_conversation.append(new_message)

    # Remove consecutive identical entries
    updated_conversation = remove_consecutive_duplicate_values(updated_conversation)

    # Reformat the data
    formatted_data = {
        "char": bot_name,
        "bot_description": data.get("bot_description", ""),
        "conversation": updated_conversation
    }

    output_filename = f"{config.get('bot_name', 'output')}_{file_index}.yaml"
    output_path = os.path.join(output_dir, output_filename)
    with open(output_path, 'w') as file:
        yaml.dump(formatted_data, file)

if __name__ == "__main__":
    input_file = sys.argv[1]
    config = load_config('config.yml')
    base_dir = os.path.dirname(os.path.dirname(input_file))
    output_dir = os.path.join(base_dir, 'partial')
    file_index = os.path.basename(input_file).split('.')[0]
    os.makedirs(output_dir, exist_ok=True)

    clean_data(input_file, config, output_dir, file_index)
