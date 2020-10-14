from io_map import IOMap
from data_layer import Redshift as SQL
from typing import Optional, List, Dict, Union

class QueryRunner(IOMap):
  query: Optional[SQL.Query]

  def __init__(self, query: Optional[Union[SQL.Query, Dict[str, any]]]=None):
    self.query = self._query_instance(query=query)

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return [
      'columns',
      'rows',
      'rowcount',
      'notices',
    ]

  def _query_instance(self, query: Optional[Union[SQL.Query, Dict[str, any]]]) -> Optional[SQL.Query]:
    if isinstance(query, dict):
      return SQL.Query(
        query=query['query'],
        substitution_parameters=tuple(query['substitution_parameters']) if 'substitution_parameters' in query else tuple()
      )
    else:
      return query

  def run(self, query: Optional[Union[SQL.Query, Dict[str, any]]]=None) -> Dict[str, any]:
    run_query = self._query_instance(query=query) if query else self.query
    layer = SQL.Layer()
    layer.connect()
    cursor = run_query.run(sql_layer=layer)
    output = {
      'columns': [],
      'rows': [],
      'rowcount': cursor.rowcount,
      'notices': layer.connection.notices,
    }
    if cursor.description is not None:
      output['columns'] = [c.name for c in cursor.description]
      output['rows'] = [list(r) for r in cursor.fetchall()]
    layer.commit()
    layer.disconnect()
    return output

class SelectKeyMapsQuery(SQL.GeneratedQuery):
  schema: str
  sets: Optional[str]
  url_pattern: Optional[str]

  def __init__(self, schema: str, sets: Optional[str]=None, url_pattern: Optional[str]=None):
    self.schema = schema
    self.sets = sets
    self.url_pattern = url_pattern
    super().__init__()

  def generate_query(self):
    condition_queries = []
    if self.sets is not None:
      condition_queries.append(SQL.Query(f'set in {SQL.Query.format_array(self.sets)}', tuple(self.sets)))
    if self.url_pattern is not None:
      condition_queries.append(SQL.Query('url ~ %s', (self.url_pattern,)))
    self.query = f'''
select set, url, key_map
from {self.schema}.maps
{"where " + ' and '.join(q.query for q in condition_queries) if condition_queries else ''};
    '''
    self.substitution_parameters = tuple(p for q in condition_queries for p in q.substitution_parameters)