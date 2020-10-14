from typing import Dict
from data_layer import Redshift as SQL
from io_map import IOMapValueType

class TagParserModel:
  maps_table = 'maps'
  maps_set = 'tag_parser'
  maps_url_prefix = 'map://tag_parser/'

class SelectTagParserMapsQuery(SQL.GeneratedQuery):
  schema: str

  def __init__(self, schema: str):
    self.schema = schema
    super().__init__()

  def generate_query(self):
    self.query = f'''
select reverse(split_part(reverse(url), %s, 1)) as name, url, key_map
from {self.schema}.{TagParserModel.maps_table}
where set = %s;
    '''
    self.substitution_parameters = (
      '/',
      TagParserModel.maps_set,
    )

class PutTagParserMapQuery(SQL.GeneratedQuery):
  schema: str
  name: str
  key_map: Dict[str, any]

  def __init__(self, schema: str, name: str, key_map: Dict[str, any]):
    self.schema = schema
    self.name = name
    self.key_map = key_map
    super().__init__()

  def generate_query(self):
    delete_query = DeleteTagParserMapQuery(
      schema=self.schema,
      name=self.name
    )
    self.query = f'''
begin transaction;
{delete_query.query}
insert into {self.schema}.{TagParserModel.maps_table} (url, set, key_map) values
(%s, %s, %s);
    '''
    self.substitution_parameters = (
      *delete_query.substitution_parameters,
      f'{TagParserModel.maps_url_prefix}{self.name}',
      TagParserModel.maps_set,
      IOMapValueType.json.format(self.key_map),
    )

class DeleteTagParserMapQuery(SQL.GeneratedQuery):
  schema: str
  name: str

  def __init__(self, schema: str, name: str):
    self.schema = schema
    self.name = name
    super().__init__()

  def generate_query(self):
    self.query = f'''
delete
from {self.schema}.{TagParserModel.maps_table}
where set = %s
and url = %s;
    '''
    self.substitution_parameters = (
      TagParserModel.maps_set,
      f'{TagParserModel.maps_url_prefix}{self.name}'
    )

class SelectTagsQuery(SQL.GeneratedQuery):
  schema: str
  include_empty: bool

  def __init__(self, schema: str, include_empty: bool=True):
    self.schema = schema
    self.include_empty = include_empty
    super().__init__()

  def generate_query(self):
    condition_query = SQL.Query('') if self.include_empty else SQL.Query(
      query='where key != %s',
      substitution_parameters=('',)
    )
    self.query = f'''
select * from {self.schema}.tags
{condition_query.query}
order by set, url, key;
    '''
    self.substitution_parameters = condition_query.substitution_parameters

class RefreshTagsQuery(SQL.GeneratedQuery):
  schema: str

  def __init__(self, schema: str):
    self.schema = schema
    super().__init__()

  def generate_query(self):
    self.query = f'''
call {self.schema}.number_urls('tags', 'url', 'number');
call {self.schema}.update_all_url_numbers(false);
refresh materialized view {self.schema}.standard_tags;
    '''

class RefreshStandardTagsQuery(SQL.GeneratedQuery):
  schema: str

  def __init__(self, schema: str):
    self.schema = schema
    super().__init__()

  def generate_query(self):
    self.query = f'''
call {self.schema}.replace_standard_tags_view();
    '''