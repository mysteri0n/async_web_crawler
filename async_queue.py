#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import os
from lxml import etree
import motor.motor_asyncio

import lxml.html as html
from urllib.parse import urlparse


CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))


def get_tag_text(t):
    return t.text


HEADERS = ["h1", "h2", "h3", "h4", "h5", "h6"]

TAG_ACTIONS = {h: get_tag_text for h in HEADERS + ["title"]}

TAG_ACTIONS.update({
    "img": lambda t: t.get("src", ""),
    "meta": lambda t: t.get("keywords", ""),
    "a": lambda t: t.get("href", "")
})


def parse_html_data(html_code, url):
    """
    Parse given HTML file and get some data from HTML tags

    :param html_code: html page code
    :param url: URL corresponding to the given HTML code
    :return: dictionary with parsed info
    """
    domain = urlparse(url).netloc
    parsed_data = {
        "url": url,
        "headers": {},
        "images": [],
        "forward_links": []
    }
    data = html.document_fromstring(html_code)

    for el in data.iter(*TAG_ACTIONS.keys()):
        tag = el.tag
        val = TAG_ACTIONS[tag](el).strip()

        if val:
            if tag in HEADERS:
                parsed_data["headers"][tag] = parsed_data["headers"].get(tag, []) + [val]
            elif tag == "img":
                parsed_data["images"].append(val)
            elif tag == "title":
                parsed_data["title"] = val
            elif tag == "meta":
                parsed_data["keywords"] = val
            elif tag == "a":
                location = urlparse(val).netloc
                if val.startswith("http") and location != domain:
                    parsed_data["forward_links"].append(val)

    return parsed_data


class Crawler(object):
    def __init__(self, wait_period=20, depth=2):
        self.results = []
        self.wait_period = wait_period  # seconds
        self.depth = depth
        self.visited_links = []
        self.client = motor.motor_asyncio.AsyncIOMotorClient()
        self.db = self.client.test_database
        self.collection = self.db['test_collection']
        self.q = asyncio.Queue()
        self.loop = asyncio.get_event_loop()

    @staticmethod
    def get_domains_data():
        with open(os.path.join(CURRENT_PATH, 'domains.json')) as f:
            domains_data = json.loads(f.read())

        return domains_data

    async def do_insert(self, document):
        """
        Insert parsed data MongoDB

        :param document: parsed html document
        :return:
        """
        await self.collection.insert(document)
        # print('result {}'.format(repr(result)))

    async def fetch_page(self):
        while not self.q.empty():
            domain, curr_url, depth, parent = self.q.get_nowait()
            async with aiohttp.ClientSession() as session:
                with aiohttp.Timeout(20):
                    async with session.get(curr_url) as response:
                        try:
                            content = await response.read()
                        except Exception as e:
                            print(e)
                        if response.status == 200 and "text/html" in response.headers["content-type"]:
                            print("URL:", curr_url, "\t\tdepth:", depth, "\tPARENT:", parent)

                            self.results.append({
                                "url": curr_url,
                                "data": content
                            })

                            try:
                                parsed_data = parse_html_data(content, curr_url)
                            except Exception as e:
                                print("Can't parse html: {}".format(e))
                            else:
                                await self.do_insert(parsed_data)

                        self.q.task_done()
                        self.visited_links.append(curr_url)

                        if depth > 0:
                            child_urls = self.get_urls(content)
                            for ch_url in child_urls:

                                if ch_url.startswith('/') and not ch_url.startswith('//'):
                                    ch_url = '{}{}'.format(domain, ch_url)

                                    if ch_url not in self.visited_links:
                                        self.q.put_nowait([domain, ch_url, depth - 1, curr_url])
                                        # await asyncio.sleep(0.2)
                                        # asyncio.ensure_future(self.fetch_page(depth - 1, domain, url))
                    
    @staticmethod
    def get_urls(html_code):
        tree = etree.HTML(html_code)
        links = tree.xpath('//a/@href')
        return list(set(links))

    async def get_pages(self, domains_data):
        for domain_data in domains_data:
            domain_url = '://'.join([domain_data["proto"], domain_data["domain"]])
            self.q.put_nowait([domain_url, domain_url, self.depth, '/root'])

            asyncio.ensure_future(self.fetch_page())

        # await asyncio.wait_for(self.q.join(), self.wait_period)
        await self.q.join()

    def get_urls_data(self):
        domains_data = self.get_domains_data()
        print(domains_data)

        future = asyncio.ensure_future(self.get_pages(domains_data))
        self.loop.run_until_complete(future)


if __name__ == "__main__":
    cr = Crawler(wait_period=100)
    cr.get_urls_data()