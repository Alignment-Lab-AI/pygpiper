import os
import yaml
import json
import requests
from multiprocessing import Pool
import time
from .utils import load_config

BATCH_SIZE = 3
MAX_RETRIES = 99999999
RETRY_DELAY = 30
TOTAL_GENERATIONS = 100000  # Set your desired total number of generations here

# Directories setup
if not os.path.exists('aug/output'):
    os.makedirs('aug/output')
if not os.path.exists('aug/malformed'):
    os.makedirs('aug/malformed')
if not os.path.exists('aug/rejected'):
    os.makedirs('aug/rejected')
if not os.path.exists('templates'):
    os.makedirs('templates')


def construct_prompt(yaml_content: str, template: str) -> str:
    """
    Constructs a prompt by appending the template to the YAML content.
    """
    yaml_string = yaml.dump(yaml_content, allow_unicode=True, default_flow_style=False)
    populated_template = template.replace('{{{row}}}', yaml_string)
    return populated_template


class OpenAI_API:
    def __init__(self):
        print('Initializing...')
        self.session = None
        self.total_generations_processed = 0
        config = load_config('config.yml')
        self.base_dir = config["input_file"]
        self.yaml_folder = os.path.join(self.base_dir, 'final')
        self.templates = self.load_templates()

    def load_templates(self) -> list[str]:
        print('Loading templates...')
        templates = []
        for filename in sorted(os.listdir('templates')):
            if filename.endswith('.txt'):
                with open(os.path.join('templates', filename), 'r') as f:
                    templates.append(f.read().strip())
        return templates

    def process_yaml_files(self):
        config = load_config('config.yml')
        base_dir = config["input_file"]
        yaml_folder = os.path.join(base_dir, 'final')
        templates = self.load_templates()
        template_idx = 0
        output_folder = 'aug/output'  # Updated output folder path

        # Load all files into the queue
        filenames = sorted([filename for filename in os.listdir(yaml_folder) if filename.endswith('.yml') or filename.endswith('.yaml')])

        with Pool(BATCH_SIZE) as p:
            p.map(self.process_file, filenames)

        print('Processing complete.')

    def process_file(self, filename):
        with open(os.path.join(self.yaml_folder, filename), 'r') as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)
            prompt = construct_prompt(yaml_content, self.templates[0])

            self.send_prompt(
                prompt,
                'aug/output',
                filename,
                yaml_content,
                self.templates[0],
            )

    def send_prompt(self, prompt, output_folder, original_filename, yaml_content, template):
        print('Sending prompt...')
        base_url = os.environ.get('OPENAI_API_BASE')
        url = f'{base_url}/chat/completions'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        }
        data = {
            'model': 'mixtral',
            'temperature': 0.1,
            'messages': [{'role': 'user', 'content': prompt}],
        }

        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                result = response.json()
                print('Received successful response.')

                augmentation = result['choices'][0]['message']['content']

                # Strip newlines
                augmentation = augmentation.replace('\n', '').replace('\r', '')

                # Detect if the response contains a JSON object
                try:
                    json_content = json.loads(augmentation)
                    # Convert JSON to YAML
                    yaml_content.update(json_content)
                except json.JSONDecodeError:
                    # If not JSON, just add it as a string
                    yaml_content['augmentation'] = augmentation

                # Write to output file
                output_filepath = os.path.join(
                    output_folder,
                    f"{original_filename.split('.')[0]}_{self.total_generations_processed}.yaml",
                )
                with open(output_filepath, 'w') as outfile:
                    yaml.dump(
                        yaml_content,
                        outfile,
                        allow_unicode=True,
                        default_flow_style=False,
                    )

                self.total_generations_processed += 1
                os.remove(os.path.join(self.yaml_folder, original_filename))
            
            elif response.status_code == 429:
                print(f'Rate limit hit. Retrying after {RETRY_DELAY} seconds...')
                time.sleep(RETRY_DELAY)
            else:
                error_content = response.text
                print(
                    f'Failed to get response, status code: {response.status_code}, error: {error_content}'
                )
        except Exception as e:
            print(f'Exception encountered: {e}')


def process_yaml_files():
    api = OpenAI_API()
    api.process_yaml_files()


if __name__ == '__main__':
    process_yaml_files()

