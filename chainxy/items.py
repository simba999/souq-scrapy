# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class ChainItem(Item):
    Brand=Field()
    Type=Field()
    Department=Field()
    Section=Field()
    Section_image=Field()
    Category=Field()
    Category_image=Field()
    Sub_category=Field()
    Sub_category_image=Field()
    Product=Field()
    Product_description=Field()
    Arabic_Product_description=Field()
    Product_quantity=Field()
    Product_price=Field()
    Product_image=Field()
    is_varient=Field()
    color=Field()
    color_code=Field()
    Size=Field()
    Specification=Field()
    Arabic_Specification=Field()
    Sold_by=Field()
    link=Field()
    ean=Field()
    
    