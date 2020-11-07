from graph import Graph
import pandas as pd
import re
import requests
from urllib3.exceptions import InsecureRequestWarning
from string import ascii_lowercase
from bs4 import BeautifulSoup
from pathlib import Path


class DictionaryGraph(Graph):
    dictionaries = ['OPTED']

    def __init__(self, name: str, word_characters: str):
        assert name in DictionaryGraph.dictionaries
        super().__init__(name)
        self.word_pattern = re.compile(f'[{word_characters}]+')
        self.word_split_pattern = re.compile(f'[^{word_characters}]')

        Path('files').mkdir(exist_ok=True)

        if not self.get('built'):
            print(f'Building graph for dictionary {self.name}')
            if self.name == 'OPTED':
                self.__build_opted()
            self.set('built', '1')
            print('Done')

    # Only words with a definition
    def vertices(self):
        return pd.Series(iter(self.adjacency_list), name='value')

    def is_word(self, s: str):
        return self.word_pattern.fullmatch(s) is not None

    def to_words(self, s: str):
        return set(filter(None, re.split(self.word_split_pattern, s.lower())))

    def add_word(self, word: str, definition: str):
        word = word.lower()
        if not self.is_word(word):
            return
        self.add_adjacencies(word, self.to_words(definition.lower()))

    def __build_opted(self):
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        print('Progress: ', end='', flush=True)
        for letter in ascii_lowercase:
            path = Path(f'files/{letter}.html')
            if path.is_file() and self.get(f'downloaded_{letter}') is not None:
                with open(path, 'r') as f:
                    soup = BeautifulSoup(f.read(), 'lxml')
            else:
                response = requests.get(f'https://www.mso.anu.edu.au/~ralph/OPTED/v003/wb1913_{letter}.html', verify=False)
                if response.status_code != requests.codes.ok:
                    raise Exception(f'Unexpected response status: {response.status_code}')
                soup = BeautifulSoup(response.text, 'lxml')
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                self.set(f'downloaded_{letter}', '1')
            for entry in soup.body.find_all('p', recursive=False):
                children = list(entry.children)
                assert len(children) == 4
                assert children[-1].startswith(') ')
                self.add_word(children[0].text, children[3][2:])
            print(letter, end='', flush=True)
        print()
        self.save()
