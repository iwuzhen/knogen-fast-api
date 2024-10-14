from faker import Faker
from fastapi import APIRouter
from functools import lru_cache
from pydantic import BaseModel
from typing import Optional
import pymongo
from zhconv import convert
import math
from collections import OrderedDict

Database = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.227").get_database("baike_demo")

router = APIRouter(
    prefix="/baike_demo",
    tags=["baike_demo"],
    responses={404: {"description": "Not found"}},
)

class UniqueQueue:
    def __init__(self):
        self.Max = 20
        self.queue = [] # 用来存储有序的元素
        self.set = set() # 用来存储已经入队的元素

    def enqueue(self, item):
        if item['id'] not in self.set: # 如果元素不在集合中，说明是新元素
            self.queue.append(item) # 将新元素加入队尾
            self.set.add(item['id']) # 将新元素加入集合
        if len(self.queue) > self.Max:
            self.dequeue()

    def dequeue(self):
        if self.queue: # 如果队列不为空
            item = self.queue.pop(0) # 取出队首元素
            self.set.remove(item) # 将该元素从集合中移除
            return item

    def is_empty(self):
        return len(self.queue) == 0

    def size(self):
        return len(self.queue)

Lately_Query = UniqueQueue()

class PageItemQuery(BaseModel):
    id: int
    namespace: Optional[int]
    title: Optional[str]
    f_title: Optional[str]
    is_redirect: Optional[int]
    len: Optional[int]
    lang: Optional[str]

class PageItemDetail(BaseModel):
    id: int
    namespace: int
    title: str
    zh_title: Optional[str]
    is_redirect: int
    len: int
    lang: str
    abstract: str
    category: list[str]
    redirect_from: Optional[str]

class BaikeDemoQueryRequests(BaseModel):
    query: str
    namespace: int
    class Config:
        json_schema_extra = {
            "example": {
                "query": "乌镇",
                "namespace": 0,
            }
        }

class BaikeDemoQueryResponse(BaseModel):
    data: list[PageItemQuery]
    class Config:
        json_schema_extra = {
            "example": {
                "data": [{
                    "id": 8285809,
                    "namespace": 0,
                    "title": "施佩尔德国行政科学大学",
                    "is_redirect": 1,
                    "len": 98,
                    "lang": "zh"
                },]
            }
        }

class BaikePageGetRequests(BaseModel):
    id: int
    lang: str
    class Config:
        json_schema_extra = {
            "example": {
                "id": 8285809,
                "lang": "zh"
            }
        }

class BaikePageGetResponse(BaseModel):
    data: PageItemDetail
    ok: bool
    class Config:
        json_schema_extra = {
            "example": {
                "data": {
                    "id": 8285809,
                    "namespace": 0,
                    "title": "施佩尔德国行政科学大学",
                    "is_redirect": 1,
                    "len": 98,
                    "lang": "zh",
                    "abstract": "this abstract",
                },
                "ok": True
            }
        }


@lru_cache(1000)
@router.post("/query",response_model=BaikeDemoQueryResponse)
def bake_query(item:BaikeDemoQueryRequests):
    """query in wiki
    Returns:
        wiki result
    """
    # ret = OpenalexEchartsResponse()
    ret = []
    # elif item.lang == "en":
    queryString = item.query.replace(' ','_')
    for doc in Database.en_page.find({'$text':{'$search':queryString},'namespace': item.namespace}).limit(5):
    # for doc in Database.en_page.find({'f_title':{ "$regex": '^'+ queryString },'namespace': item.namespace}).limit(5):
        doc["lang"] = "en"
        doc["id"] = doc['_id']
        # doc["title"] = doc["title"]
        del(doc['_id'])
        ret.append(doc)

    # if item.lang == "zh":
    # for doc in Database.zh_page.find({'$text':{'$search':queryString},'namespace': item.namespace}).limit(5):
    for doc in Database.zh_page.find({'f_title':{ "$regex": '^'+ queryString },'namespace': item.namespace}).limit(5):
        doc["lang"] = "zh"
        doc["id"] = doc['_id']
        # doc["title"] = doc["title"]
        del(doc['_id'])
        ret.append(doc)

    return {
        'data': ret,
    }


@lru_cache(101)
@router.post("/page",response_model=BaikePageGetResponse)
def bake_page_query(item:BaikePageGetRequests):

    prefix = ""
    if item.lang == "zh":
        prefix = "zh"
    elif item.lang == "en":
        prefix = "en"
    print("item", item)

    # get source doc
    
    ret = Database[f'{prefix}_page'].find_one({"_id": item.id})
    if not ret:
        return {"data":{},"ok":False}
    ret['id'] = ret['_id']
    del(ret['_id'])

    # try to redirect page
    if ret["is_redirect"] !=0:
        pipeline = [
            {
                "$match":{"from":item.id}
            },
            {
                "$lookup": {
                    'from': f'{prefix}_page',
                    'localField': "title",
                    'foreignField': "title",
                    'as': "page"
                }
            },
            {
                '$unwind': "$page"
            },
            {"$limit": 1}
        ]
        cur = Database[f'{prefix}_redirect'].aggregate(pipeline)
        try:
            doc = next(cur)
        except:
            return {"data":{},"ok":False}
        if doc:
            page = doc['page']
            page['lang'] = item.lang
            page["id"] = page['_id']
            del(page['_id'])
            page['redirect_from'] = ret['title']
            ret = page

    # try to get category
    category = []
    for doc in Database[f'{prefix}_categorylinks'].find({'from': ret['id']}):
        category.append(doc['to'])

    ret['lang'] = item.lang
    ret['category'] = category
    ret['abstract'] = ""

    if (ret['lang'] == 'en'):
        # try to get zh name
        doc = Database[f'{prefix}_langlinks'].find_one({'from': ret['id'],'lang': "zh"})
        if doc:
            try:
                ret['zh_title'] = convert(doc['title'], 'zh-cn')
            except Exception as e:
                print(e)
                ret['zh_title'] = doc['title']
    ret['title'] = ret['title']
    # print("ret item", ret)
    Lately_Query.enqueue(ret)
    return {
        'data':ret,
        'ok': True
    }


@router.get("/lately_search")
def bake_page_query():
    return {
        "data": Lately_Query.queue,
        "ok": True
    }