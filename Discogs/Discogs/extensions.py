from scrapy.utils.httpobj import urlparse_cached

class HttpCachePolicy(object):
    def __init__(self, settings):
        self.ignore_schemes = settings.getlist('HTTPCACHE_IGNORE_SCHEMES')
        self.ignore_http_codes = [int(x) for x in settings.getlist('HTTPCACHE_IGNORE_HTTP_CODES')]
        
    def should_cache_response(self, response, request):
        return response.status == 200

    def should_cache_request(self, request):
        return urlparse_cached(request).scheme not in self.ignore_schemes

    def is_cached_response_fresh(self, cachedresponse, request):
        return True

    def is_cached_response_valid(self, cachedresponse, response, request):
        return True