import pandas as pd
import numpy as np
import sqlite3
from scipy.sparse import csr_matrix
from typing import Tuple


# Unweighted directed graph
class Graph:
    version = '1'

    def __init__(self, name):
        self.name = name
        self.adjacency_list = {}

        if 'tables' not in self.__dict__:
            self.tables = {}

        self.tables, tables = {
            'data': [
                'key TEXT NOT NULL PRIMARY KEY',
                'value TEXT NOT NULL',
            ],
            'vertices': [
                'id INTEGER PRIMARY KEY',
                'value TEXT NOT NULL',
            ],
            'edges': [
                'id1 INTEGER NOT NULL',
                'id2 INTEGER NOT NULL',
                'FOREIGN KEY(id1) REFERENCES vertices(id)',
                'FOREIGN KEY(id2) REFERENCES vertices(id)',
                'UNIQUE(id1, id2)',
            ],
        }, self.tables
        self.tables.update(tables)

        self.indices = {
            'vertices_value': 'vertices(value)',
            'edges_id1': 'edges(id1)',
            'edges_id2': 'edges(id2)',
        }

        self.conn = sqlite3.connect(f'{name}.sqlite')
        if self.get('version') != Graph.version:
            for name in self.tables:
                self.conn.execute(f'DROP TABLE IF EXISTS {name}')
            for name in self.indices:
                self.conn.execute(f'DROP INDEX IF EXISTS {name}')
        for name in self.tables:
            self.create_table(name)
        for name, on in self.indices.items():
            self.conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {on}')
        self.set('version', Graph.version)
        self.conn.commit()

    def __del__(self):
        self.conn.close()

    def create_table(self, name):
        schema = f'CREATE TABLE IF NOT EXISTS {name} (' + ','.join('\n    ' + line for line in self.tables[name]) + '\n)'
        self.conn.execute(schema)

    def query_value(self, sql: str, parameters):
        c = self.conn.cursor()
        c.row_factory = lambda cursor, row: row[0]
        return c.execute(sql, parameters).fetchone()

    def get(self, key):
        try:
            return self.query_value('SELECT value from data WHERE key=?', (key,))
        except sqlite3.OperationalError:  # no such table: data
            return None

    def set(self, key, value):
        self.conn.execute('INSERT OR REPLACE INTO data VALUES (?,?)', (key, value))
        self.conn.commit()

    def add_adjacencies(self, i, j):
        if i in self.adjacency_list:
            self.adjacency_list[i] |= j
        else:
            self.adjacency_list[i] = j

    # All vertices mentioned in adjacency list
    def vertices(self):
        vertices = set(self.adjacency_list)
        for _,  j in self.adjacency_list.items():
            vertices |= j
        return pd.Series(iter(vertices), name='value')

    def save(self):
        vertices = self.vertices()
        edges = pd.DataFrame(((i, k) for (i, j) in self.adjacency_list.items() for k in j), columns=['value1', 'value2'])
        vertices2 = pd.Series(vertices.index.values, index=vertices, name='id2')
        edges = pd.merge(edges, vertices2, left_on='value2', right_index=True)
        vertices2.name = 'id1'
        edges = pd.merge(edges, vertices2, left_on='value1', right_index=True)[['id1', 'id2']]
        vertices.to_sql('vertices', self.conn, if_exists='append', index_label='id')
        edges.to_sql('edges', self.conn, if_exists='append', index=False)

    def size(self):
        return self.query_value('SELECT COUNT(*) FROM vertices'), self.query_value('SELECT COUNT(*) FROM edges')

    def adjacency_matrix(self, value: str) -> Tuple[pd.Series, csr_matrix]:
        w = self.query_value('SELECT id FROM vertices WHERE value=?', (value,))
        if w is None:
            raise Exception('Vertex does not exist')
        # Vertices of the neighbourhood graph G_w
        self.conn.execute('''CREATE TEMPORARY VIEW IF NOT EXISTS vertices_{0} AS
            SELECT id FROM (SELECT id2 AS id, COUNT(id1) AS count FROM edges WHERE id2 IN (
                SELECT id1 AS id FROM edges WHERE id2={0} UNION SELECT id2 FROM edges WHERE id1={0}
            ) GROUP BY id2) WHERE count < 1000
            UNION VALUES({0})
        '''.format(w))
        vertices = pd.read_sql(f'SELECT vertices.id, vertices.value FROM vertices JOIN vertices_{w} ON vertices.id = vertices_{w}.id', self.conn, index_col='id')
        n = vertices.size
        vertices['i'] = np.arange(0, n)
        edges = pd.read_sql(f'SELECT * FROM edges WHERE id1 IN vertices_{w} AND id2 IN vertices_{w}', self.conn)
        return vertices.set_index('i')['value'], csr_matrix((np.ones(edges.shape[0]), (edges['id1'].map(vertices['i']), edges['id2'].map(vertices['i']))), shape=(n, n))  # , pd.concat((edges['id1'].map(vertices['i']), edges['id2'].map(vertices['i'])), axis=1)


class WeightedGraph(Graph):
    def __init(self, name):
        if 'tables' not in self.__dict__:
            self.tables = {}
        self.tables = {
            'edges': [
                'id1 INTEGER NOT NULL',
                'id2 INTEGER NOT NULL',
                'weight REAL NOT NULL',
                'FOREIGN KEY(id1) REFERENCES vertices(id)',
                'FOREIGN KEY(id2) REFERENCES vertices(id)',
                'UNIQUE(id1, id2)',
            ],
        }.update(self.tables)
        super().__init__(name)
