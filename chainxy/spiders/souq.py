import scrapy
import json
import os
from scrapy.spiders import Spider
from scrapy.http import FormRequest
from scrapy.http import Request
from chainxy.items import ChainItem
from lxml import etree
# from selenium import webdriver
from lxml import html
import uuid
import urllib
from collections import OrderedDict
import math
import csv
import re
import math
import pdb

from peewee import *

db = SqliteDatabase('souq_777.db')

class BaseModel(Model):
    class Meta:
        database = db

class ProductSouq(BaseModel):
    Brand = CharField(null=True)
    Type = CharField(null=True)
    Department = CharField(null=True)
    Section = CharField(null=True)
    Section_image = CharField(null=True)
    Category = CharField(null=True)
    Category_image = CharField(null=True)
    Sub_category = CharField(null=True)
    Sub_category_image = CharField(null=True)
    Product = CharField(null=True)
    Product_description = TextField(null=True)
    Product_quantity = CharField(null=True)
    Product_price = CharField(null=True)
    Product_image = CharField(null=True)
    is_varient = CharField(null=True)
    color = CharField(null=True)
    color_code = CharField(null=True)
    Size = CharField(null=True)
    Specification = CharField(null=True)
    Sold_by = CharField(null=True)
    link = CharField(null=True)
    ean = CharField(null=True)

db.connect()
db.create_tables([ProductSouq])


class souq(scrapy.Spider):
    name = 'souq'
    domain = 'https://uae.souq.com/ae-en'
    sub_link_list = []
    fieldnames = ["Brand", "Type", "Department", "Section", "Section_image", "Category", "Category_image", "Sub_category", "Sub_category_image", "Product", "Product_description", "Product_quantity",     "Product_price", "Product_image", "is_varient", "color", "color_code", "Size",     "Specification", "Sold_by", "link", "ean", "Category_link", "image_links", "Section_id", 'download_slot', 'detail_link', 'download_timeout', 'depth', 'download_latency']

    def start_requests(self):
        init_url = 'https://uae.souq.com/ae-en/'
        yield scrapy.Request(url=init_url, callback=self.parse_category)

    def parse_category(self, response):
        level0_list = response.xpath('//ul[@id="megaMenuNav"]//li[contains(@class, "level0")]')
        # print(level0_list)
        for level0_item in level0_list[1:]:
            item = dict()
            department_name = self.validate(level0_item.xpath('./a/text()').extract_first())
            if department_name is None or department_name == '':
                department_name = 'fashion'

            item['Department'] = department_name
            level1_list = []
            try:
                level1_list = level0_item.xpath('.//div[@class="level1-container"]')
            except:
                level1_list = []
            if len(level1_list) > 0:
                level1_items = level1_list[0].xpath('.//li[contains(@class, "level1")]')
                for level1_item in level1_items:
                    section = self.validate(level1_item.xpath('./a/text()').extract_first())
                    item['Section'] = section
                    level2_list = level1_item.xpath('./ul[contains(@class, "menu-content")]//div[contains(@class, "container")]//div[contains(@class, "columns")]//div[@class="row"]')

                    for level2_item in level2_list:
                        category_name = self.validate(level2_item.xpath('.//h4//a/text()').extract_first())
                        category_link = level2_item.xpath('.//li//a/@href').extract()
                        if category_name.lower() == 'womens':
                            category_name = 'women'
                        if category_name.lower() == 'mens':
                            category_name = 'men'

                        item['Category'] = category_name
                        for detail_link in category_link:
                            item['Category_link'] = self.validate(detail_link)
                            if '/c/' in item['Category_link']:
                                yield scrapy.Request(url=item['Category_link'], callback=self.get_json)
                            else:
                                yield scrapy.Request(url=item['Category_link'], callback=self.parse_category_html, meta={'entry_item': item})
                            # yield scrapy.Request(url=item['Category_link'], callback=self.parse_category_html, meta={'entry_item': item})
            else:
                level2_list = level0_item.xpath('.//ul[contains(@class, "menu-content")]//div[contains(@class, "columns")]//div[@class="row"]//div[contains(@class, "columns")]//div[@class="row"]')
                item['Section'] = ''
                for level2_item in level2_list:
                    category_name = self.validate(level2_item.xpath('.//h4//text()').extract_first())
                    category_link = level2_item.xpath('.//li//a/@href').extract()
                    if category_name.lower() == 'womens':
                        category_name = 'women'
                    if category_name.lower() == 'mens':
                        category_name = 'men'

                    item['Category'] = category_name
                    for detail_link in category_link:
                        item['Category_link'] = self.validate(detail_link)
                        if '/c/' in item['Category_link']:
                            yield scrapy.Request(url=item['Category_link'], callback=self.get_json)
                        else:
                            yield scrapy.Request(url=item['Category_link'], callback=self.parse_category_html, meta={'entry_item': item})
                        # yield scrapy.Request(url=item['Category_link'], callback=self.parse_category_html, meta={'entry_item': item})

    def get_json(self, response):
        category = response.url.split('/c/')[1].split('?')[0]
        total_count = int(response.xpath("//li[@class='total']//span/text()").extract_first())
        pageNumber = int(math.ceil(total_count / 30))
        # pdb.set_trace()
        for page in range(1, pageNumber+1):
            detail_link = 'https://supermarket.souq.com/ae-en/search?campaign_id=' + category + '&page=' + str(page) + '&sort=best'
            req = scrapy.Request(url=detail_link, callback=self.get_detail_json)
            req.meta['link'] = response.url
            yield req

    def get_detail_json(self, response):
        try:
            json_data = json.loads(response.body)['data']
            category_link = response.meta['link']

            for entry in json_data:
                item = dict()
                item['Category_link'] = category_link
                item['Department'] = ''
                item['Category'] = ''
                item['Section'] = ''
                yield scrapy.Request(url=entry['item_url'], callback=self.parse_detail_html_page, meta={'entry_val': item})
        except:
            pass


    def parse_detail_page(self, response):
        metadata = response.meta['entry']
        item = dict()
        item = metadata

        color_items = response.xpath('//span[contains(@class, "has-tip")]')
        color_values = []
        item['Product'] = self.validate(response.xpath('//div[contains(@class, "product-title")]//h1/text()').extract_first())
        item['image_links'] = response.xpath('//span[contains(@class, "has-tip")]/a/@data-url').extract()
        item['Sold_by'] = self.validate(response.xpath('//span[@class="unit-seller-link"]//a/b/text()').extract_first())
        item['Sub_category'] = self.validate(response.xpath('//div[contains(@class, "product-title")]//span//a[2]/text()').extract_first())
        item['Product_quantity'] = ''
        quantity = response.xpath('//div[@class="unit-labels"]//b//span/text()').extract_first()
        if quantity:
            quantity_vals = re.findall(r'\b\d+\b', quantity.encode('utf8'))
            if len(quantity_vals) > 0:
                item['product_quantity'] = quantity_vals[0]

        spec_titles = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dt//text()').extract()
        spec_values = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dd//text()').extract()
        
        # GET SPEC LIST INTO STRING
        num = 1
        tmp = {}
        for spec in spec_titles:
            try:
                val = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dd[' + str(num) + ']/text()').extract_first() or ''
                tmp[spec.encode('utf-8')] = val.encode('utf-8')
                num = num + 1
            except:
                pass

        item['Specification'] = tmp
        description = response.xpath('//div[@id="description-full"]//text()').extract()
        if len(description) == 0:
            description = response.xpath('//div[@id="description-short"]//text()').extract()

        str_desc = ''
        for str_item in description:
            str_desc = str_desc + str_item
            # str_desc = str_desc + self.validate(str_item)
        item['Product_description'] = self.validate(str_desc)

        for color_item in color_items:
            tmp = dict()
            tmp['color'] = self.validate(color_item.xpath('./@data-value').extract_first())
            tmp['color_image'] = self.validate(color_item.xpath('./a/@style').extract_first().split('background-image:url(')[1].split(');')[0])
            tmp['color_link'] = self.validate(color_item.xpath('./a/@data-url').extract_first())
            yield scrapy.Request(url=tmp['color_link'], callback=self.parse_detail_image, meta={'entry': item, 'color': tmp['color']})            

    def parse_detail_image(self, response):
        metadata = dict()
        metadata = response.meta['entry']
        color = response.meta['color']
        detail_link = response.meta['detail_link']

        slick_dots_images = response.xpath('//a[@data-open="product-gallery-modal"]//img/@data-url').extract()
        images_list = []
        for slick_image in slick_dots_images:
            base_dir = os.path.dirname(__file__)
            resource = urllib.urlopen(slick_image.encode('utf-8'))
            image_name = uuid.uuid4().hex + '.' + resource.info().type.split('/')[1]
            output = open(base_dir + '/images/' + image_name,"wb")
            output.write(resource.read())
            output.close()
            images_list.append(image_name)

        item = ChainItem()

        item = metadata
        item['color'] = color
        item['link'] = detail_link

        if len(metadata['Size']) > 1:
            image_cnt = len(images_list)
            num = 0
            for size in metadata['Size']:
                item['Size'] = size
                item['is_varient'] = 1
                item['Product_image'] = images_list[num]
                if num == (image_cnt -1):
                    num = 0
                else:
                    num = num + 1

                ProductSouq.create(Brand=self.has_target_key('Brand', item) \
                    ,Type=self.has_target_key('Type', item) \
                    ,Department=self.has_target_key('Department', item) \
                    ,Section=self.has_target_key('Section', item) \
                    ,Section_image=self.has_target_key('Section_image', item) \
                    ,Category=self.has_target_key('Category', item) \
                    ,Category_image=self.has_target_key('Category_image', item) \
                    ,Sub_category=self.has_target_key('Sub_category', item) \
                    ,Sub_category_image=self.has_target_key('Sub_category_image', item) \
                    ,Product=self.has_target_key('Product', item) \
                    ,Product_description=self.has_target_key('Product_description', item) \
                    ,Product_quantity=self.has_target_key('Product_quantity', item) \
                    ,Product_price=self.has_target_key('Product_price', item) \
                    ,Product_image=self.has_target_key('Product_image', item) \
                    ,is_varient=self.has_target_key('is_varient', item) \
                    ,color=self.has_target_key('color', item) \
                    ,color_code=self.has_target_key('color_code', item) \
                    ,Size=self.has_target_key('Size', item) \
                    ,Specification=self.has_target_key('Specification', item) \
                    ,Sold_by=self.has_target_key('Sold_by', item) \
                    ,link=self.has_target_key('link', item) \
                    ,ean=self.has_target_key('ean', item))
                yield item
                    
        else:
            try:
                item['Size'] = metadata['Size'][0]
            except:
                item['Size'] = ''
            item['is_varient'] = 0
            for single_image in images_list:
                item['Product_image'] = single_image

                ProductSouq.create(Brand=self.has_target_key('Brand', item) \
                    ,Type=self.has_target_key('Type', item) \
                    ,Department=self.has_target_key('Department', item) \
                    ,Section=self.has_target_key('Section', item) \
                    ,Section_image=self.has_target_key('Section_image', item) \
                    ,Category=self.has_target_key('Category', item) \
                    ,Category_image=self.has_target_key('Category_image', item) \
                    ,Sub_category=self.has_target_key('Sub_category', item) \
                    ,Sub_category_image=self.has_target_key('Sub_category_image', item) \
                    ,Product=self.has_target_key('Product', item) \
                    ,Product_description=self.has_target_key('Product_description', item) \
                    ,Product_quantity=self.has_target_key('Product_quantity', item) \
                    ,Product_price=self.has_target_key('Product_price', item) \
                    ,Product_image=self.has_target_key('Product_image', item) \
                    ,is_varient=self.has_target_key('is_varient', item) \
                    ,color=self.has_target_key('color', item) \
                    ,color_code=self.has_target_key('color_code', item) \
                    ,Size=self.has_target_key('Size', item) \
                    ,Specification=self.has_target_key('Specification', item) \
                    ,Sold_by=self.has_target_key('Sold_by', item) \
                    ,link=self.has_target_key('link', item) \
                    ,ean=self.has_target_key('ean', item))

                yield item

    def parse_category_html(self, response):
        metadata = response.meta['entry_item']
        detail_links = response.xpath('//div[contains(@class, "grid-list")]//div[contains(@class,"single-item")]//a[contains(@class, "sPrimaryLink")]/@href').extract()
        for detail in detail_links:
            yield scrapy.Request(url=detail, callback=self.parse_detail_html_page, meta={'entry_val': metadata})

        next_link = response.xpath('//ul[contains(@class, "srp-pagination")]//li[contains(@class, "pagination-next")]//a')
        if len(next_link) != 0:
            yield scrapy.Request(url=next_link.xpath('./@href').extract_first(), callback=self.parse_category_html, meta={'entry_item': metadata})


    def parse_detail_html_page(self, response):
        metadata = response.meta['entry_val']
        item = dict()
        item = metadata
        item['ean'] = response.xpath('//div[@id="productTrackingParams"]/@data-ean').extract_first()
        souq_data = ProductSouq.select().where(ProductSouq.ean==item['ean'])
        if souq_data:
            pass
        else:
            item['Type'] = "product"
            item['Department'] = metadata['Department']
            try:
                item['Section'] = metadata['Section']
            except:
                pass
            item['Category_link'] = response.url
            item['Category'] = metadata['Category']
            item['Product'] = self.validate(response.xpath('//div[contains(@class, "product-title")]//h1/text()').extract_first())
            
            price = response.xpath('//h3[@class="price is sk-clr1"]//text()').extract()
            price_val = ''
            for price_str in price:
                price_val = price_val + price_str.strip()
            item['Product_price'] = price_val.replace('SAR', ' SAR')
            item['Size'] = response.xpath('//div[contains(@class, "item-connection")]//a/text()').extract()

            color_items = response.xpath('//span[contains(@class, "has-tip")]')
            color_values = []
            item['image_links'] = response.xpath('//span[contains(@class, "has-tip")]/a/@data-url').extract()
            item['Sold_by'] = self.validate(response.xpath('//span[@class="unit-seller-link"]//a/b/text()').extract_first())
            item['Sub_category'] = self.validate(response.xpath('//div[contains(@class, "product-title")]//span//a[2]/text()').extract_first())
            item['Product_quantity'] = ''
            quantity = response.xpath('//div[@class="unit-labels"]//b//span/text()').extract_first()
            if quantity:
                quantity_vals = re.findall(r'\b\d+\b', quantity.encode('utf8'))
                if len(quantity_vals) > 0:
                    item['Product_quantity'] = quantity_vals[0]

            spec_titles = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dt//text()').extract()
            spec_values = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dd//text()').extract()
            
            # GET SPEC LIST INTO STRING
            num = 1
            tmp = OrderedDict()
            for spec in spec_titles:
                try:
                    val = response.xpath('//div[@id="specs-full"]//dl[@class="stats"]//dd[' + str(num) + ']/text()').extract_first() or ''
                    tmp[spec.encode('utf-8')] = val.encode('utf-8')
                    if spec == u'Brand':
                        item['Brand'] = val
                    num = num + 1
                except:
                    pass

            item['Specification'] = json.dumps(tmp)
        
            description = response.xpath('//div[@id="description-full"]//text()').extract()
            if len(description) == 0:
                description = response.xpath('//div[@id="description-short"]//text()').extract()

            str_desc = ''
            for str_item in description:
                str_desc = str_desc + str_item

            item['Product_description'] = self.validate(str_desc)
            for color_item in color_items:
                tmp = dict()
                tmp['color'] = self.validate(color_item.xpath('./@data-value').extract_first())
                tmp['color_image'] = self.validate(color_item.xpath('./a/@style').extract_first().split('background-image:url(')[1].split(');')[0])
                tmp['color_link'] = self.validate(color_item.xpath('./a/@data-url').extract_first())
                cate_request = scrapy.Request(url=tmp['color_link'], callback=self.parse_detail_image)
                new_tmp = dict()
                for item_val in item:
                    new_tmp[item_val] = item[item_val]
                cate_request.meta['entry'] = new_tmp
                cate_request.meta['color'] = tmp['color']
                cate_request.meta['detail_link'] = tmp['color_link']
                yield cate_request

    def validate(self, item):
        try:
            return item.strip().replace('\r', '').replace(u'\u2019', "'").replace(u'\2022', '').encode('utf-8')
        except:
            return ''

    def eliminate_space(self, items):
        tmp = []
        for item in items:
            if self.validate(item) != '':
                tmp.append(self.validate(item))
        return tmp

    def has_target_key(self, val, dic):
        if val in dic:
            return dic[val]
        else:
            return ''

