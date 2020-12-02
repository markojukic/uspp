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
            print('Done.')

    # Only words with a definition
    def vertices(self):
        return pd.Series(iter(self.adjacency_list), name='value')

    def is_word(self, s: str):
        return self.word_pattern.fullmatch(s) is not None

    def to_words(self, s: str):
        return set(filter(None, re.split(self.word_split_pattern, s.lower())))

    def __build_opted(self):
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        word_classes = {}

        print('Processing: ', end='', flush=True)
        for letter in ascii_lowercase:
            path = Path(f'files/{letter}.html')
            if path.is_file():
                with open(path, 'r') as f:
                    soup = BeautifulSoup(f.read(), 'lxml')
            else:
                response = requests.get(f'https://www.mso.anu.edu.au/~ralph/OPTED/v003/wb1913_{letter}.html', verify=False)
                if response.status_code != requests.codes.ok:
                    raise Exception(f'Unexpected response status: {response.status_code}')
                soup = BeautifulSoup(response.text, 'lxml')
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
            for entry in soup.body.find_all('p', recursive=False):
                children = list(entry.children)
                assert len(children) == 4
                assert children[-1].startswith(') ')
                assert children[2].name == 'i'
                word = children[0].text.lower()
                if self.is_word(word):
                    self.add_adjacencies(word, self.to_words(children[3][2:].lower()))
                    word_class = children[2].text
                    if word_class in word_classes:
                        word_classes[word_class].add(word)
                    else:
                        word_classes[word_class] = {word}
            print(letter, end='', flush=True)

        defined_words = set(self.adjacency_list)
        nouns = word_classes['n.']
        verbs = set()
        for word_class, words in word_classes.items():
            if word_class.startswith('v.'):
                verbs.update(words)
        for word, definition_words in self.adjacency_list.items():
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
            self.adjacency_list[word] = filtered_words

        print('\nSaving...')
        self.save()
