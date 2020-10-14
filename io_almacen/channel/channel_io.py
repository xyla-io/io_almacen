from __future__ import annotations
from enum import Enum
from typing import Optional
from io_channel import IOEntityGranularity

class Channel(Enum):
  apple_search_ads = 'apple_search_ads'
  google_ads = 'google_ads'
  snapchat = 'snapchat'
  tiktok = 'tiktok'
  facebook = 'facebook'

  @classmethod
  def from_name(cls, name: str) -> Optional[Channel]:
    for channel in cls:
      if name in [channel.value, channel.display_name]:
        return channel
    return None

  @property
  def display_name(self) -> str:
    if self is Channel.apple_search_ads:
      return 'Apple'
    elif self is Channel.google_ads:
      return 'Google'
    elif self is Channel.snapchat:
      return 'Snapchat'
    elif self is Channel.tiktok:
      return 'TikTok'
    elif self is Channel.facebook:
      return 'Facebook'

  def entity_granularity_for_name(self, name: str) -> Optional[IOEntityGranularity]:
    for granularity in IOEntityGranularity:
      if name == granularity.value:
        return granularity
    if name in ['adset', 'adsquad']:
      return IOEntityGranularity.adgroup
    if name in ['creative_set']:
      return IOEntityGranularity.ad
    return None

def channel_entity_url(channel: str, entity: str, entity_id: str):
  channel_enum = Channel.from_name(channel)
  entity_granularity = channel_enum.entity_granularity_for_name(entity)
  return f'channel_entity://{channel_enum.value}/{entity_granularity.value}/{entity_id}'