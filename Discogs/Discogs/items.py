# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class ResponseData(scrapy.Item):
    album = scrapy.Field()
    artist = scrapy.Field()

class AlbumData(scrapy.Item):
    artist_name = scrapy.Field()
    album_name = scrapy.Field()
    profile = scrapy.Field()
    track_list = scrapy.Field()
    album_version = scrapy.Field()
    album_credits = scrapy.Field()

class ArtistData(scrapy.Item):
    artist_name = scrapy.Field()
    sites = scrapy.Field()
    artist_credits = scrapy.Field()
    artist_vocals = scrapy.Field()
