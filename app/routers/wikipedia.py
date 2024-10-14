from faker import Faker
from fastapi import APIRouter
from functools import lru_cache
from pydantic import BaseModel
from typing import Optional
import pymongo
from zhconv import convert
import wikipediaapi
import math
import datetime
from collections import OrderedDict


database = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.227").get_database("wikipedia_cache")
zh_summary_collection = database.zh_summary
en_summary_collection = database.en_summary

zh_summary_collection.create_index([('title', pymongo.ASCENDING),('date', pymongo.DESCENDING)],background=True)
en_summary_collection.create_index([('title', pymongo.ASCENDING),('date', pymongo.DESCENDING)],background=True)
# {
#     '_id','title', 'summary', 'date'
# }

proxy = {'https': 'http://192.168.1.230:10811'}
EN_API = wikipediaapi.Wikipedia(
    language='en',
    proxies=proxy,
    user_agent="wikipediaapi",
)
ZH_API = wikipediaapi.Wikipedia(
    language='zh',
    proxies=proxy,
    user_agent="wikipediaapi",
)


router = APIRouter(
    prefix="/wikipedia",
    tags=["wikipedia"],
    responses={404: {"description": "Not found"}},
)

lru_cache(100)
def get_title(title, lang):
    if lang == "zh":
        doc = zh_summary_collection.find_one({'title':title}, sort = [('date', -1),])
        if doc:
            return doc.get('summary')
        else:
            # try get page from wikipedia
            page = ZH_API.page(title)
            try:
                summary = page.summary.replace("()","").replace("（）","")
                summary = convert(summary,'zh-cn')
                zh_summary_collection.insert_one({'title':title, 'date':datetime.datetime.now(),'summary': summary })
                return summary
            except Exception as e:
                print("summary get fail,", title, e)
                return ""
    elif lang=='en':
        doc = en_summary_collection.find_one({'title':title},  sort = [('date', -1),])
        if doc == True:
            return doc.get('summary')
        else:
            # try get page from wikipedia
            page = EN_API.page(title)
            try:
                summary = page.summary.replace("()","").replace("（）","")
                en_summary_collection.insert_one({'title':title, 'date':datetime.datetime.now(),'summary': summary })
                return summary
            except Exception as e:
                print("summary get fail,", title, e)
                return ""


class WikipediaSummaryQuery(BaseModel):
    title: str
    lang: str

class WikipediaSummaryQueryRequests(BaseModel):
    WikipediaSummaryQuery
    class Config:
        json_schema_extra = {
            "example": {
                "lang": "zh",
                "title": "乌镇",
            }
        }

@router.post("/summary")
def bake_query(item:WikipediaSummaryQuery):
    summary = get_title(item.title, item.lang)
    return summary