import json
import pandas as pd

from enum import Enum
from datetime import datetime
from typing import Optional, Dict, List
from io_map import IOMap, IOMapKey, IOMapOption, IOMapValueType, IOMapEach, IOMapZip, IOMapPassthrough, IOMapConstantKey
from subir.upload import Uploader
from .tag_query import TagParserModel, RefreshTagsQuery, RefreshStandardTagsQuery
from ..query import QueryRunner, SelectKeyMapsQuery

class TagError(Exception):
  pass

class TagParserNotFoundError(TagError):
  parser_url: str

  def __init__(self, parser_url: str):
    self.parser_url = parser_url
    super().__init__(f'Tag parser not found: {parser_url}')

class TagUpdateMode(Enum):
  tag = 'tag'
  url = 'url'

class NameTagsProcessor(IOMap):
  schema: str
  urls_to_names: Optional[Dict[str, str]]
  name_tags: Optional[Dict[str, Dict[str, str]]]
  update_mode: Optional[str]

  def __init__(self, schema: str):
    self.schema = schema

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: KeyMapFetcher,
        IOMapKey.construct.value: {
          'schema': 'construct.schema',
          'map_sets': ['str.tag_parser'],
        },
        IOMapKey.output.value: 'run.parser_key_maps',
      },
      {
        IOMapKey.map.value: NameTagsParser,
        IOMapKey.input.value: {
          'names': 'run.names',
          'parser_url': 'input.parser_url',
          'parser_key_maps': 'run.parser_key_maps',
        },
        IOMapKey.output.value: 'run.name_tags',
      },
      {
        IOMapKey.map.value: TagsUpdater,
        IOMapKey.options.value: {
          IOMapOption.enabled.value: 'str.run.perform_update',
        },
        IOMapKey.construct.value: {
          'schema': 'construct.schema',
        },
        IOMapKey.input.value: {
          'urls_to_tags': 'run.output',
          'update_mode': 'run.update_mode',
        },
      },
    ]

  @property
  def names(self) -> List[str]:
    return list(self.urls_to_names.values())

  @property
  def output(self) -> List[str]:
    return {
      u: self.name_tags[n]
      for u, n in self.urls_to_names.items()
    }

  @property
  def perform_update(self) -> bool:
    return self.update_mode and self.output

  def run(self, urls_to_names: Dict[str, str], parser_url: str, update_mode: Optional[str]=None):
    return super().run(
      urls_to_names=urls_to_names,
      parser_url=parser_url,
      update_mode=update_mode
    )

class TagsUpdater(IOMap):
  schema: str
  tag_set: Optional[str]

  def __init__(self, schema: str, tag_set: Optional[str]=None):
    self.schema = schema
    self.tag_set = tag_set

  @classmethod
  def get_key_maps(self) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: TagsRefresher,
        IOMapKey.options.value: {
          IOMapOption.enabled.value: 'str.run.should_refresh',
        },
        IOMapKey.construct.value: {
          'schema': 'construct.schema',
        },
      },
    ]

  def run(self, urls_to_tags: Dict[str, Dict[str, str]], update_mode: str, should_refresh: bool=True) -> List[Dict[str, any]]:
    update_mode_enum = TagUpdateMode(update_mode)
    merge_column_names = [
      'url',
      'set',
    ]
    if update_mode_enum is TagUpdateMode.url:
      merge_replace = True
      urls_to_tags = {
        k: v if v else {'': ''}
        for k, v in urls_to_tags.items()
      }
    else:
      merge_replace = False
      merge_column_names.append('key')

    tag_updates = [
      {
        **({'set': self.tag_set} if self.tag_set is not None else {}),
        'url': url,
        'key': k,
        'value': v,
      }
      for url, tags in urls_to_tags.items()
      for k, v in tags.items()
    ]
    if not tag_updates:
      return tag_updates

    data_frame = pd.DataFrame(tag_updates)
    data_frame['modified'] = datetime.utcnow()
    uploader = Uploader()
    uploader.upload_data_frame(
      schema_name=self.schema,
      table_name='tags',
      merge_column_names=merge_column_names,
      data_frame=data_frame,
      column_type_transform_dictionary={},
      merge_replace=merge_replace
    )
    return super().run(
      should_refresh=should_refresh,
      output=tag_updates
    )

class TagsRefresher(IOMap):
  schema: str
  query: RefreshTagsQuery
  verbose: bool

  @classmethod
  def get_calcuate_keys(self) -> List[str]:
    return [
      'query',
    ]

  @classmethod
  def get_key_maps(self) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: QueryRunner,
        IOMapKey.construct.value: 'calculate.query',
        IOMapKey.output.value: {
          'notices': 'run.output'
        },
      },
    ]

  def __init__(self, schema: str, verbose: bool=False):
    self.schema = schema
    self.query = RefreshTagsQuery(schema=self.schema)
    self.verbose = verbose

  def run(self) -> List[str]:
    output = super().run()
    if self.verbose and output:
      notices = '\n'.join(output)
      print(f'Tags refresh query:\n{self.query.substituted_query}\ngenerated {len(output)} noticies:\n{notices}')

class KeyMapFetcher(IOMap):
  schema: Optional[str]
  map_sets: Optional[List[str]]
  map_pattern: Optional[str]
  rows: Optional[List[List[str]]]

  def __init__(self, schema: Optional[str]=None, map_sets: Optional[List[str]]=None, url_pattern: Optional[str]=None):
    self.schema = schema
    self.map_sets = map_sets
    self.url_pattern = url_pattern

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: QueryRunner(),
        IOMapKey.input.value: 'run.query',
        IOMapKey.output.value: {
          'rows': 'run.rows',
        },
      },
      {
        IOMapKey.map.value: IOMapZip(),
        IOMapKey.input.value: [
          'run.urls',
          'run.key_maps',
        ],
        IOMapKey.output.value: 'run.output',
      },
    ]  

  @property
  def urls(self) -> List[str]:
    return [r[1] for r in self.rows]

  @property
  def key_maps(self) -> List[str]:
    return [IOMapValueType.json.parse(r[2]) for r in self.rows]

  def run(self) -> List[Dict[str, any]]:
    query = SelectKeyMapsQuery(
      schema=self.schema,
      sets=self.map_sets,
      url_pattern=self.url_pattern
    )
    output = super().run(query=query)
    return output

class NameTagsParser(IOMap):
  parser_key_maps: Optional[Dict[str, Dict[str, any]]]

  def __init__(self, parser_key_maps: Dict[str, Dict[str, any]]=None):
    self.parser_key_maps = parser_key_maps

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: IOMapEach(),
        IOMapKey.input.value: {
          'items': 'input.names',
          'key_map': 'run.parser_key_map',
        },
        IOMapKey.output.value: 'run.tags',
      },
      {
        IOMapKey.map.value: IOMapZip(),
        IOMapKey.input.value: {
          'keys': 'input.names',
          'values': 'input.tags',
        },
        IOMapKey.output.value: 'run.output',
      },
    ]

  def run(self, names: List[str], parser_url: str, parser_key_maps: Dict[str, Dict[str, any]]=None) -> Dict[str, Dict[str, str]]:
    run_parser_key_maps = parser_key_maps if parser_key_maps is not None else self.parser_key_maps
    if parser_url not in run_parser_key_maps:
      raise TagParserNotFoundError(parser_url)
    parser_key_map = parser_key_maps[parser_url]
    parser_provider_map = IOMapConstantKey(
      key_constant=f'join.["str.iokeymap.{TagParserModel.maps_url_prefix}", "run.0"]',
      fallback_keys=[f'json.{IOMapValueType.json.format(IOMapConstantKey(key_constant="json.{}")._construct_key_map)}'],
    )
    with IOMap._local_registries():
      IOMap._register_map_identifiers([
        'io_map.util/IOMapConstantKey',
        'io_channel.parse/IOSequenceParser',
        'io_channel.parse/IORegexParser',
        'io_channel.parse/IOSwitchParser',
      ])
      IOMap._register_key_maps(key_maps={
        **run_parser_key_maps,
        'parser_provider': parser_provider_map._construct_key_map,
      })
      name_tags = super().run(
        names=names,
        parser_key_map=parser_key_map
      )
    return name_tags

class StandardTagsUpdater(IOMap):
  schema: str
  tag_set: Optional[str]
  url: str
  parser_key_maps: Optional[str]=None

  def __init__(self, schema: str, tag_set: Optional[str]='standard', url: str='standard://parsers'):
    self.schema = schema
    self.tag_set = tag_set
    self.url = url

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: KeyMapFetcher,
        IOMapKey.construct.value: {
          'schema': 'construct.schema',
          'map_sets': ['str.tag_parser'],
        },
        IOMapKey.output.value: 'run.parser_key_maps',
      },
      {
        IOMapKey.map.value: TagsUpdater,
        IOMapKey.construct.value: {
          'schema': 'construct.schema',
          'tag_set': 'construct.tag_set',
        },
        IOMapKey.input.value: {
          'urls_to_tags': 'run.parser_tags',
          'update_mode': 'str.url',
        },
      },
      {
        IOMapKey.map.value: QueryRunner,
        IOMapKey.construct.value: 'calculate.query',
        IOMapKey.output.value: {
          'notices': 'run.output'
        },
      },
    ]

  @property
  def query(self) -> RefreshStandardTagsQuery:
    return RefreshStandardTagsQuery(schema=self.schema)

  @property
  def parser_tags(self) -> Dict[str, Dict[str, str]]:
    parser_key_maps = self.parser_key_maps
    if not parser_key_maps:
      labels = []
    else:
      parser_provider_map = IOMapConstantKey(
        key_constant=f'join.["str.iokeymap.{TagParserModel.maps_url_prefix}", "run.0"]',
        fallback_keys=[f'json.{IOMapValueType.json.format(IOMapConstantKey(key_constant="json.{}")._construct_key_map)}'],
      )
      with IOMap._local_registries():
        IOMap._register_map_identifiers([
          'io_map.util/IOMapConstantKey',
          'io_channel.parse/IOSequenceParser',
          'io_channel.parse/IORegexParser',
          'io_channel.parse/IOSwitchParser',
        ])
        IOMap._register_key_maps(key_maps={
          **parser_key_maps,
          'parser_provider': parser_provider_map._construct_key_map,
        })
        parser_maps = [
          IOMap._generate_instantiated_key_map(k)
          for k in parser_key_maps.values()
        ]
        labels = {
          o
          for m in parser_maps
          for o in m[IOMapKey.map.value].labels
        }
    return {
      self.url: {
        l: ''
        for l in labels
      },
    }
    
      
    