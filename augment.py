import os
import multiprocessing
import requests
import time
import yaml
import re

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


class OpenAI_API:
    def __init__(self):
        print('Initializing...')
        self.session = None
        self.total_generations_processed = 0
        self.file_queue = multiprocessing.Queue()

    def load_templates(self):
        print('Loading templates...')
        templates = []
        for filename in sorted(os.listdir('templates')):
            if filename.endswith('.txt'):
                with open(os.path.join('templates', filename), 'r') as f:
                    templates.append(f.read().strip())
        return templates

    def construct_prompt(self, yaml_content, template):
        """
        Constructs a prompt by appending the template to the YAML content.

        :param yaml_content: A string containing the content of a YAML file.
        :param template: A string template to be appended under the YAML content.
        :return: A string with the YAML content followed by the template.
        """
        yaml_string = yaml.dump(yaml_content, allow_unicode=True, default_flow_style=False)
        populated_template = template.replace('{{{row}}}', yaml_string)
        return populated_template

    def process_yaml_files(self):
        templates = self.load_templates()
        yaml_folder = 'aug/final'
        template_idx = 0

        output_folder = 'aug/output'  # Updated output folder path

        # Load all files into the queue
        for filename in sorted(os.listdir(yaml_folder)):
            if filename.endswith('.yml') or filename.endswith('.yaml'):
                self.file_queue.put(filename)

        while not self.file_queue.empty() and self.total_generations_processed < TOTAL_GENERATIONS:
            tasks = []
            for _ in range(BATCH_SIZE):
                if self.file_queue.empty() or self.total_generations_processed >= TOTAL_GENERATIONS:
                    break

                filename = self.file_queue.get()
                with open(os.path.join(yaml_folder, filename), 'r') as f:
                    yaml_content = yaml.load(f, Loader=yaml.FullLoader)
                    prompt = self.construct_prompt(yaml_content, templates[template_idx])

                    task = multiprocessing.Process(
                        target=self.send_prompt,
                        args=(
                            prompt,
                            output_folder,
                            filename,
                            yaml_content,
                            templates[template_idx],
                        ),
                    )
                    tasks.append(task)

            for task in tasks:
                task.start()

            for task in tasks:
                task.join()

        print('Processing complete.')

    def send_prompt(self, prompt, output_folder, original_filename, yaml_content, template):
        print('Sending prompt...')
        base_url = os.environ.get('OPENAI_API_BASE')
        url = f'{base_url}/v1/chat/completions'
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

                # TODO: replace with funcchain
                augmentation = result['choices'][0]['message']['content']

                # Detect if the response contains a list-like structure and process it
                list_match = re.search(
                    r'(\w+)\s*:\s*(\[.*?\])\s*(?![^\[]*\])', augmentation, re.DOTALL
                )
                if list_match:
                    list_name = list_match.group(1)
                    list_content_str = list_match.group(2)
                    list_content = yaml.safe_load(list_content_str)
                    yaml_content[list_name] = list_content
                else:
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
                os.remove(os.path.join('aug/final', original_filename))

            elif response.status_code == 429:
                print(f'Rate limit hit. Retrying after {RETRY_DELAY} seconds...')
                time.sleep(RETRY_DELAY)
            else:
                error_content = response.text
                print(
                    f'Failed to get response, status code: {response.status_code}, error: {error_content}'
                )
                if response.status_code == 403:
                    self.handle_rejection(prompt, error_content)
        except Exception as e:
            print(f'Exception encountered: {e}')


if __name__ == '__main__':
    api = OpenAI_API()
    api.process_yaml_files()
