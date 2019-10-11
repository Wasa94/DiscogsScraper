import scrapy
from Discogs.items import ResponseData

class DiscozSpider(scrapy.Spider):
    name = 'discogs_spider'
    allowed_domains = ['discogs.com']

    url_base = [
        'https://www.discogs.com/search/?limit=250&country_exact=Serbia&decade=1990&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Serbia&decade=2000&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Serbia&decade=2010&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1920&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1930&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1940&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1950&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=2000&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=2010&page=1',
        'https://www.discogs.com/search/?limit=250&sort=title%2Cdesc&decade=1970&country_exact=Yugoslavia&page=1',
        'https://www.discogs.com/search/?limit=250&sort=title%2Casc&decade=1970&country_exact=Yugoslavia&page=1',
        'https://www.discogs.com/search/?limit=250&sort=title%2Cdesc&decade=1980&country_exact=Yugoslavia&page=1',
        'https://www.discogs.com/search/?limit=250&sort=title%2Casc&decade=1980&country_exact=Yugoslavia&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1960&page=1',
        'https://www.discogs.com/search/?limit=250&country_exact=Yugoslavia&decade=1990&page=1'
    ]


    def start_requests(self):
        for url in self.url_base:
            yield scrapy.Request(url, callback = self.parse_discogs)
        #yield scrapy.Request(url = self.url_base[14], callback = self.parse_discogs)


    def parse_discogs(self, response):
        for page in response.xpath('//a[@class="search_result_title"]/@href'):
            yield response.follow(page, callback = self.parse_album)

        page = response.xpath('//a[@class="pagination_next"]')
        if len(page) > 0:
            yield response.follow(page[0], callback = self.parse_discogs)


    def parse_artist(self, response):
        data = ResponseData()
        data['artist'] = response
        yield data

    def parse_album(self, response):
        data = ResponseData()
        data['album'] = response
        yield data

        page = response.xpath('//div[@class="profile"]/h1/span[1]/span/a/@href')
        if len(page) > 0:
            yield response.follow(page[0], callback = self.parse_artist)