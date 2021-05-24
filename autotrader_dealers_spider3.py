import time
import csv
import json
import os
import re
import scrapy
import hashlib
import requests
from math import ceil
from random import randint
import urllib.parse
from lxml.html import fromstring
from copy import deepcopy
from scrapy.crawler import CrawlerProcess


POSTCODE = "WC2N 5DU"

DOWNLOAD_DELAY = 0.1
THREADS = 10

STORM_PROXY = [
    # client's proxies
    # '37.48.118.90:13042',
    # '83.149.70.159:13042'
    # public proxies
    '50.21.183.143:3128',
    '51.158.165.18:8811'
]

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) '\
             'AppleWebKit/537.36 (KHTML, like Gecko) '\
             'Chrome/81.0.4044.129 Safari/537.36'

LOG_FILE = 'logs/autotrader_dealers_spider.log'
LOG_FILE = None


class ExtractItem(scrapy.Item):
    Name = scrapy.Field()
    Reviews = scrapy.Field()
    Stars = scrapy.Field()
    Address = scrapy.Field()
    Cars_Listed = scrapy.Field()
    Page_Link = scrapy.Field()
    Ph_no_1 = scrapy.Field()
    Ph_no_2 = scrapy.Field()


class AutoTraderSpider(scrapy.Spider):
    name = "autotrader_dealers_spider"
    total_urls = 1
    base_url = 'https://www.autotrader.co.uk/'
    headers = {
        'referer': base_url,
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'user-agent': USER_AGENT,
    }
    post_code = POSTCODE.lower().replace(' ', '')
    query_params = {
        'advertising-location': 'at_cars',
        'postcode': post_code,
        'radius': '1500',
        'forSale': 'on',
        'toOrder': 'on',
        'forSale': 'on',
        'toOrder': 'on',
        'sort': 'with-retailer-reviews',
    }
    ajax_search_url = f'{base_url}car-dealers/search?'

    def start_requests(self, ):
        url = f'{self.base_url}car-dealers'
        yield scrapy.Request(
            url=url,
            callback=self.parse_search,
            headers=self.headers
        )

    def parse_search(self, response):
        makes = response.xpath(
            '//select[@name="make"]/option[not(text()="Any")]'
            '/text()').extract()
        #makes = ['AUDI']
        for make in makes:
            query_params = deepcopy(self.query_params)
            query_params.update({
                'make': make,
                'page': 1
            })
            params = urllib.parse.urlencode(query_params)
            result_url = self.ajax_search_url + params
            yield scrapy.Request(
                url=result_url,
                callback=self.parse_results,
                headers=response.request.headers,
                meta={'make': make, 'page': 1}
            )

    def parse_results(self, response):
        make = response.meta['make']
        page = response.meta['page']
        try:
            json_response = json.loads(response.text)
            html_response = fromstring(json_response['html'])
        except Exception:
            match = re.findall(
                r'.*?(\<script.*\<\/nav\>)',
                response.text,
                re.DOTALL
            )
            if not match:
                return
            html_response = fromstring(match[0])
        results = html_response.xpath('//article[@class="dealerList__item"]')
        for result in results:
            name = result.xpath('header/a/span/text()')
            if not name:
                continue
            item = ExtractItem()
            item['Name'] = name[0].strip()

            reviews = result.xpath(
                'header//meta[@itemprop="ratingValue"]/@content')
            item['Stars'] = reviews[0].strip() if reviews else None

            stars = result.xpath(
                'header//meta[@itemprop="ratingCount"]/@content')
            item['Reviews'] = stars[0].strip() if stars else None

            address = result.xpath(
                'a//p[@class="dealerList__itemAddress"]/text()')
            if address:
                address = address[0].strip().replace('\n', '')
                address = re.sub(r'\s{2,}', ' ', address)
                item['Address'] = address

            cars_listed = result.xpath(
                'a//span[@class="dealerList__itemCountNumber"]/text()')
            item['Cars_Listed'] = cars_listed[0].strip()\
                if cars_listed else None

            page_link = result.xpath('a/@href')
            if page_link:
                url = page_link[0]
                item['Page_Link'] = "https://www.autotrader.co.uk" + url

                yield scrapy.Request(
                    url=item['Page_Link'],
                    callback=self.parse_dealer_info,
                    headers=response.request.headers,
                    meta={'item': item, 'make': make, 'page': page}
                )
            else:
                self.logger.info(
                    f'Done -> #Make: {make} #Page: {page} '
                    f'#dealer: {item["Name"]}')
                yield item

        next_page_index = html_response.xpath(
            '//li[contains(@class,"pagination--li")]'
            '/a[span[text()="Next"]]/@data-paginate')
        if next_page_index:
            query_params = deepcopy(self.query_params)
            query_params.update({
                'make': make,
                'page': next_page_index[0]
            })
            params = urllib.parse.urlencode(query_params)
            result_url = self.ajax_search_url + params
            yield scrapy.Request(
                url=result_url,
                callback=self.parse_results,
                headers=response.request.headers,
                meta={'make': make, 'page': next_page_index[0]}
            )

    def parse_dealer_info(self, response):
        item = response.meta['item']
        make = response.meta['make']
        page = response.meta['page']
        self.logger.info(
            f'Done -> #Make: {make} #Page: {page} '
            f'#dealer: {item["Name"]}')
        phones = response.xpath(
            '//section[@class="dealer-profile-telephone-number-container"]'
            '/a/text()').extract()
        for index, phone in enumerate(phones, 1):
            item[f"Ph_no_{index}"] = phone
        yield item


def settings_storm():
    settings = {
        'ITEM_PIPELINES': {
            'pipelines_dealers.AutoTraderDealerExtractPipeline': 300,
        },
        "DOWNLOADER_MIDDLEWARES": {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        },
        'DOWNLOAD_DELAY': DOWNLOAD_DELAY,
        'CONCURRENT_REQUESTS': THREADS,
        'CONCURRENT_REQUESTS_PER_DOMAIN': THREADS,
        'ROTATING_PROXY_LIST': STORM_PROXY,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'RETRY_TIMES': 20,
        'LOG_LEVEL': 'INFO',

    }
    if LOG_FILE:
        settings.update({
            'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            'LOG_FILE': LOG_FILE,
        })
    return settings


def run_spider():
    settings = settings_storm()
    process = CrawlerProcess(settings)
    process.crawl(AutoTraderSpider)
    process.start()


if __name__ == '__main__':
    if LOG_FILE and os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    run_spider()
