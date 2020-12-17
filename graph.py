import pandas as pd
import numpy as np
import sqlite3
from scipy.sparse import csr_matrix
from typing import Tuple, Any, Union, List


class Conn:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_table(self, table_name: str, lines: List[str]) -> None:
        schema = f'CREATE TABLE IF NOT EXISTS {table_name} (' + ','.join('\n    ' + line for line in lines) + '\n)'
        self.conn.execute(schema)

    def drop_table(self, table_name: str) -> None:
        self.conn.execute(f'DROP TABLE IF EXISTS {table_name}')

    def create_index(self, index_name: str, table_name: str, indexed_columns: List[str]):
        self.conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({', '.join(indexed_columns)})")

    def drop_index(self, index_name: str) -> None:
        self.conn.execute(f'DROP INDEX IF EXISTS {index_name}')

    def query_value(self, sql: str, parameters: Tuple[Any] = ()):
        c = self.conn.cursor()
        c.row_factory = lambda cursor, row: row[0]
        return c.execute(sql, parameters).fetchone()


class KeyValueStore(Conn):
    schema = [
        'key TEXT NOT NULL PRIMARY KEY',
        'value TEXT NOT NULL',
    ]

    def __init__(self, database: Union[str, sqlite3.Connection], table_name: str) -> None:
        super().__init__(database)
        self.table_name = table_name
        self.create_table(table_name, KeyValueStore.schema)

    def get(self, key: str):
        try:
            return self.query_value(f'SELECT value from {self.table_name} WHERE key=?', (key,))
        except sqlite3.OperationalError:  # no such table: data
            return None

    def set(self, key: str, value: str):
        self.conn.execute('INSERT OR REPLACE INTO data VALUES (?,?)', (key, value))
        self.conn.commit()


class GraphWriter(Conn):
    tables = {
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
    }
    indices = {
        'vertices_value': ('vertices', ['value']),
        'edges_id1': ('edges', ['id1']),
        'edges_id2': ('edges', ['id2']),
    }

    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)
        self.adjacency_list = {}

        for table_name in self.tables:
            self.drop_table(table_name)
        for index_name in self.indices:
            self.drop_index(index_name)
        for table_name, lines in self.tables.items():
            self.create_table(table_name, lines)
        for index_name, (table_name, indexed_columns) in self.indices.items():
            self.create_index(index_name, table_name, indexed_columns)
        self.conn.commit()

    def add_adjacencies(self, i, j):
        if i in self.adjacency_list:
            self.adjacency_list[i] |= j
        else:
            self.adjacency_list[i] = j

    # Save induced subgraph
    def save(self, vertices: List[str]):
        vertices = pd.Series(vertices, name='value')
        edges = pd.DataFrame(
            ((i, k) for (i, j) in self.adjacency_list.items() for k in j),
            columns=['value1', 'value2']
        )
        vertices2 = pd.Series(vertices.index.values, index=vertices, name='id2')
        edges = pd.merge(edges, vertices2, left_on='value2', right_index=True)
        vertices2.name = 'id1'
        edges = pd.merge(edges, vertices2, left_on='value1', right_index=True)[['id1', 'id2']]
        vertices.to_sql('vertices', self.conn, if_exists='append', index_label='id')
        edges.to_sql('edges', self.conn, if_exists='append', index=False)


class GraphReader(Conn):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)

    # Number of vertices and edges
    def size(self):
        return self.query_value('SELECT COUNT(*) FROM vertices'), self.query_value('SELECT COUNT(*) FROM edges')

    def vertices(self):
        return pd.read_sql('SELECT * FROM vertices', self.conn, index_col='id')

    def edges(self):
        return pd.read_sql('SELECT * FROM edges', self.conn)

    # Returns all vertices and edges as pandas DataFrames
    def to_pandas(self):
        return self.vertices(), self.edges()

    def indegrees(self):
        return pd.read_sql(
            'SELECT id2 AS id, COUNT(id1) AS indegree FROM edges GROUP BY id2',
            self.conn,
            index_col='id'
        )

    # Generates the neighbourhood graph G_w
    # Methods:
    #   1 - Uses all edges
    #   2 - Removes vertices i from G_w with indegree(i) >= 1000
    #   3, f - Assigns weight to edges i->j equal to 1/f(indegree(j))
    def neighbourhood(self, value: str, method=1, f=None) -> Tuple[pd.Series, csr_matrix]:
        w = self.query_value('SELECT id FROM vertices WHERE value=?', (value,))
        if w is None:
            raise Exception('Vertex does not exist')

        # Vertices of the neighbourhood graph G_w
        vertices_view = f'vertices_{w}_{method}'
        if method == 1 or method == 3:
            self.conn.execute('''
                CREATE TEMPORARY VIEW IF NOT EXISTS {1} AS
                SELECT id1 AS id FROM edges WHERE id2={0} UNION SELECT id2 FROM edges WHERE id1={0} UNION VALUES({0})
            '''.format(w, vertices_view))
        elif method == 2:
            self.conn.execute('''
                CREATE TEMPORARY VIEW IF NOT EXISTS {1} AS
                SELECT id FROM (SELECT id2 AS id, COUNT(id1) AS count FROM edges WHERE id2 IN (
                    SELECT id1 AS id FROM edges WHERE id2={0} UNION SELECT id2 FROM edges WHERE id1={0}
                ) GROUP BY id2) WHERE count < 1000
                UNION VALUES({0})
            '''.format(w, vertices_view))

        # Vertices and their values
        if method == 1 or method == 2:
            vertices = pd.read_sql(
                'SELECT vertices.id, vertices.value FROM vertices JOIN {0} ON vertices.id = {0}.id'.format(
                    vertices_view), self.conn, index_col='id')
        elif method == 3:
            vertices = pd.read_sql('''
                SELECT vertices.id, vertices.value, in_degrees.count FROM
                (vertices JOIN {1} ON vertices.id = {1}.id)
                LEFT JOIN
                (SELECT id2 AS id, COUNT(id1) AS count FROM edges WHERE id2 IN {1} GROUP BY id2) AS in_degrees
                ON vertices.id = in_degrees.id
            '''.format(w, vertices_view), self.conn, index_col='id')

        # Edges
        edges = pd.read_sql('SELECT * FROM edges WHERE id1 IN {0} AND id2 IN {0}'.format(vertices_view), self.conn)

        # Edge weights
        weights = np.ones(edges.shape[0])
        if method == 3:
            if f is not None:
                vertices['count'] = f(vertices['count'])
            weights /= edges['id2'].map(vertices['count'])

        # New index for vertices
        vertices['i'] = np.arange(0, vertices.shape[0])

        # Return the reindexed vertices and a sparse adjacency matrix
        return (
            vertices.set_index('i')['value'],
            csr_matrix(
                (weights, (edges['id1'].map(vertices['i']), edges['id2'].map(vertices['i']))),
                shape=(vertices.shape[0], vertices.shape[0])
            )
        )
