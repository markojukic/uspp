from graph import KeyValueStore, GraphReader, GraphWriter
import re
import requests
from urllib3.exceptions import InsecureRequestWarning
from string import ascii_lowercase
from bs4 import BeautifulSoup
from pathlib import Path
import sqlite3


class DictionaryGraph(GraphReader):
    # Current version
    version = '4'
    # Supported dictionaries
    dictionaries = ['OPTED']
    # Only words with these characters are considered
    word_characters = 'a-z'
    word_pattern = re.compile(f'[{word_characters}]+')
    word_split_pattern = re.compile(f'[^{word_characters}]')

    def __init__(self, name: str):
        assert name in DictionaryGraph.dictionaries
        conn = sqlite3.connect(f'{name}.sqlite')
        super().__init__(conn)
        self.data = KeyValueStore(self.conn, 'data')

        if self.data.get('dictionary_version') != DictionaryGraph.version:
            print(f'Building graph for dictionary {name}...')
            # Rebuild graph
            writer = GraphWriter(self.conn)
            if name == 'OPTED':
                self.build_opted(writer)
            self.data.set('dictionary_version', DictionaryGraph.version)
            print('Done.')

    def __del__(self):
        self.conn.close()

    @classmethod
    def is_word(cls, s: str):
        return cls.word_pattern.fullmatch(s) is not None

    @classmethod
    def to_words(cls, s: str):
        return set(filter(None, re.split(cls.word_split_pattern, s.lower())))

    def build_opted(self, writer: GraphWriter):
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        Path('files').mkdir(exist_ok=True)
        word_classes = {}

        print('Processing letters: ', end='', flush=True)
        for letter in ascii_lowercase:
            path = Path(f'files/{letter}.html')
            if path.is_file() and self.data.get(f'downloaded_{letter}') == '1':  # File downloaded
                with open(path, 'r') as f:
                    soup = BeautifulSoup(f.read(), 'lxml')
            else:  # File not downloaded
                response = requests.get(
                    f'https://www.mso.anu.edu.au/~ralph/OPTED/v003/wb1913_{letter}.html',
                    verify=False
                )
                if response.status_code != requests.codes.ok:
                    raise Exception(f'Unexpected response status: {response.status_code}')
                soup = BeautifulSoup(response.text, 'lxml')
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                self.data.set(f'downloaded_{letter}', '1')
            for entry in soup.body.find_all('p', recursive=False):
                children = list(entry.children)
                assert len(children) == 4
                assert children[-1].startswith(') ')
                assert children[2].name == 'i'
                word = children[0].text.lower()
                if DictionaryGraph.is_word(word):
                    writer.add_adjacencies(word, DictionaryGraph.to_words(children[3][2:].lower()))
                    word_class = children[2].text
                    if word_class in word_classes:
                        word_classes[word_class].add(word)
                    else:
                        word_classes[word_class] = {word}
            print(letter, end='', flush=True)

        defined_words = set(writer.adjacency_list)
        nouns = word_classes['n.']
        verbs = set()
        for word_class, words in word_classes.items():
            if word_class.startswith('v.'):
                verbs.update(words)
        for word, definition_words in writer.adjacency_list.items():
            filtered_words = set()
            for i in definition_words:
                if i in defined_words:
                    filtered_words.add(i)
                # Plural nouns
                elif i.endswith('s') and i[:-1] in nouns:  # car -> cars
                    filtered_words.add(i[:-1])
                elif i.endswith('es') and i[:-2] in nouns:  # bus -> buses
                    filtered_words.add(i[:-2])
                elif i.endswith('ves') and i[:-3] + 'f' in nouns:  # wolf -> wolves
                    filtered_words.add(i[:-3] + 'f')
                elif i.endswith('ies') and i[:-3] + 'y' in nouns:  # city -> cities
                    filtered_words.add(i[:-3] + 'y')
                # Verbs
                elif i.endswith('d') and i[:-1] in verbs:  # live -> lived
                    filtered_words.add(i[:-1])
                elif i.endswith('ed') and i[:-2] in verbs:  # play -> played
                    filtered_words.add(i[:-2])
                elif i.endswith('ied') and i[:-3] + 'y' in verbs:  # try -> tried
                    filtered_words.add(i[:-3] + 'y')
                elif i.endswith('ing') and i[:-3] in verbs:  # play -> playing
                    filtered_words.add(i[:-3])
            writer.adjacency_list[word] = filtered_words

        # Save induced subgraph with only defined words
        print('\nSaving...')
        writer.save(list(writer.adjacency_list))
