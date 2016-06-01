#!/usr/bin/env python3
import asyncio
import datetime
import json
import os
import traceback
from concurrent.futures._base import TimeoutError
from urllib.parse import urlparse
import random

import aiohttp
import lxml.html as html
import motor.motor_asyncio
import requests
from lxml import etree

CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))

HEADERS = ["h1", "h2", "h3", "h4", "h5", "h6"]

TAG_ACTIONS = {h: lambda t: t.text for h in HEADERS + ["title"]}

TAG_ACTIONS.update({
    "img": lambda t: t.get("src", ""),
    "meta": lambda t: t.get("keywords", ""),
    "a": lambda t: t.get("href", "")
})


def parse_html_data(html_code: str, url:str):
    """
    Parse given HTML data and get some data from HTML tags

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
        val = TAG_ACTIONS[tag](el)
        val = val.strip() if isinstance(val, str) else ''

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

    parsed_data['forward_links'].sort()
    return parsed_data


class Crawler(object):
    """Async web crawler just to demonstrate asyncio power"""
    def __init__(self, depth: int = 2, is_verbose: bool = False):
        self.depth = depth
        self.requested_urls = set()
        self.is_verbose = is_verbose

        # mongoDB connection
        self.client = motor.motor_asyncio.AsyncIOMotorClient()
        self.db = self.client.test_database
        self.collection = self.db['test_collection']

        self.loop = asyncio.get_event_loop()
        self.counter = 0
        self.counter_noct = 0
        self.success_urls = set()

    @staticmethod
    def get_domains_data():
        """Load crawlers config"""
        with open(os.path.join(CURRENT_PATH, 'domains.json')) as f:
            domains_data = json.loads(f.read())

        return domains_data

    async def do_insert(self, document):
        """
        Insert parsed data MongoDB

        :param document: parsed html document
        :return:
        """
        await self.collection.insert_one(document)

    async def fetch_page(self, session: aiohttp.ClientSession, domain: str, curr_url: str, depth: int):
        """Retrieve single URL and its child pages asynchronously"""
        content = None
        self.requested_urls.add(curr_url)

        res_st, res_head = 200, {'Content-Type': 'text/html'}
        try:
            # wait random interval before making the request from 0 to 10 sec to not DDOS requested sites
            # of course it makes crawler becomes a bit slower
            await asyncio.sleep(random.random() * 10)
            async with session.get(curr_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                content = await response.read()
                res_st = response.status
                res_head = response.headers
        except aiohttp.ClientError as e:
            print(f"GET URL exception: {e}")
        except TimeoutError:
            print(f"Timeout exceeded for url {curr_url}")
        except Exception:
            print(f"Unexpected error happened: {traceback.format_exc()}")
        else:
            if self.is_verbose:
                print(f"Got response from page {curr_url}")
            self.success_urls.add(curr_url)

        if content and res_st < 400 and "text/html" in res_head["Content-Type"]:
            try:
                parsed_data = parse_html_data(content, curr_url)
                await self.do_insert(parsed_data)
            except Exception as e:
                print("Can't parse html: {}".format(e))

            self.counter += 1
            if depth > 0:
                child_urls = self.get_urls(content)
                if self.is_verbose:
                    print("Child urls:\n", "\n".join(sorted(child_urls)))

                child_tasks = []
                for ch_url in child_urls:
                    if ch_url.startswith('/') and not ch_url.startswith('//'):
                        ch_url = '{}{}'.format(domain, ch_url)
                        if ch_url not in self.requested_urls:
                            if self.is_verbose:
                                print(f"Create task for child page {ch_url} of parent {curr_url}")
                            child_tasks.append(self.fetch_page(session, domain, ch_url, depth - 1))
                            self.requested_urls.add(ch_url)
                    if ch_url.startswith(domain) and ch_url not in self.requested_urls:
                        if self.is_verbose:
                            print(f"Create task for child page {ch_url} of parent {curr_url}")
                        child_tasks.append(self.fetch_page(session, domain, ch_url, depth - 1))
                        self.requested_urls.add(ch_url)

                await asyncio.gather(*child_tasks)
        else:
            self.counter_noct += 1
                    
    @staticmethod
    def get_urls(html_code: str):
        """Extract URLs from HTML page links within 'a' TAG"""
        tree = etree.HTML(html_code)
        links = tree.xpath('//a/@href')
        return sorted(set(links))

    async def get_pages(self):
        """Retrieve data from URLs in config file in asynchronous way"""
        domains_data = self.get_domains_data()
        async with aiohttp.ClientSession() as session:
            tasks = []

            for domain_data in domains_data:
                domain_url = '://'.join([domain_data["proto"], domain_data["domain"]])

                tasks.append(
                    asyncio.create_task(self.fetch_page(session, domain_url, domain_url, self.depth))
                )

            await asyncio.gather(*tasks)

    def get_urls_data(self):
        """Entry point for asynchronous retrieval function"""
        future = asyncio.ensure_future(self.get_pages())

        self.loop.run_until_complete(future)
        print("SUCCESS COUNTER:", self.counter)
        print("NO CONTENT COUNTER:", self.counter_noct)
        print("TOTAL COUNTER:", self.counter_noct + self.counter)
        print(f"REQUESTED COUNTER: {len(self.requested_urls)}")
        print(f"SUCCESS URLS COUNTER: {len(self.success_urls)}")

        if self.is_verbose:
            success_urls = '\n'.join(sorted(self.success_urls))
            print(f"SUCCESS URLS:\n{success_urls}")

        self.client.close()
        self.loop.close()

    def fetch_page_sync(self, domain, curr_url, depth):
        """Simplified synchronous version of fetch_page"""
        content = None
        self.requested_urls.add(curr_url)
        try:
            res = requests.get(curr_url, timeout=10)
            content = res.content
        except Exception:
            print(f"Unexpected error happened: {traceback.format_exc()}")
        else:
            if self.is_verbose:
                print(f"Got response from page {curr_url}")
            self.success_urls.add(curr_url)

        if content and res.status_code == 200 and "text/html" in res.headers["content-type"]:
            # try:
            #     parsed_data = parse_html_data(content, curr_url)
            #     # await self.do_insert(parsed_data)
            # except Exception as e:
            #     print("Can't parse html: {}".format(e))

            self.counter += 1
            if depth > 0:
                child_urls = self.get_urls(content)

                for ch_url in child_urls:
                    if ch_url.startswith('/') and not ch_url.startswith('//'):
                        ch_url = '{}{}'.format(domain, ch_url)
                        if ch_url not in self.requested_urls:
                            if self.is_verbose:
                                print(f"Create task for child page {ch_url} of parent {curr_url}")
                            self.fetch_page_sync(domain, ch_url, depth - 1)
                            self.requested_urls.add(ch_url)
                    if ch_url.startswith(domain) and ch_url not in self.requested_urls:
                        if self.is_verbose:
                            print(f"Create task for child page {ch_url} of parent {curr_url}")
                        self.fetch_page_sync(domain, ch_url, depth - 1)
                        self.requested_urls.add(ch_url)

        else:
            self.counter_noct += 1

    def get_pages_sync(self):
        """Retrieve data from URLs in config file in synchronous way"""
        domains_data = self.get_domains_data()

        for domain_data in domains_data:
            domain_url = '://'.join([domain_data["proto"], domain_data["domain"]])
            self.fetch_page_sync(domain_url, domain_url, self.depth)

    def get_urls_data_sync(self):
        """Entry point for synchronous version of get_urls_data"""
        self.get_pages_sync()

        print("SUCCESS COUNTER:", self.counter)
        print("NO CONTENT COUNTER:", self.counter_noct)
        print("TOTAL COUNTER:", self.counter_noct + self.counter)
        print(f"REQUESTED COUNTER: {len(self.requested_urls)}")
        print(f"SUCCESS URLS COUNTER: {len(self.success_urls)}")


if __name__ == "__main__":
    # run asynchronous version
    st_time = datetime.datetime.now()
    cr = Crawler()
    cr.get_urls_data()
    print(f"DELTA = {datetime.datetime.now() - st_time}")

    # run synchronous version
    # st_time = datetime.datetime.now()
    # cr = Crawler()
    # cr.get_urls_data_sync()
    # print(f"DELTA = {datetime.datetime.now() - st_time}")
