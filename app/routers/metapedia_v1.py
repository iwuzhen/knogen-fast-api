from faker import Faker
from fastapi import APIRouter, HTTPException
from functools import lru_cache
from pydantic import BaseModel, validator
from typing import Optional
import pymongo
from zhconv import convert
import datetime
import math
from collections import OrderedDict
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase, ManagedTransaction, Record

Database = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.227").get_database("baike_demo")
ES8 = Elasticsearch("http://192.168.1.227:9200")

def get_driver():
    driver = GraphDatabase.driver(f"bolt://192.168.1.227:17688", auth=("neo4j", "neo4j-test"))
    return driver

router = APIRouter(
    prefix="/metapedia/v1",
    tags=["metapedia"],
    responses={404: {"description": "Not found"}},
)

class UniqueQueue:
    def __init__(self):
        self.Max = 20
        self.queue = [] # 用来存储有序的元素
        self.set = set() # 用来存储已经入队的元素

    def enqueue(self, item):
        if item.id not in self.set: # 如果元素不在集合中，说明是新元素
            self.queue.append(item) # 将新元素加入队尾
            self.set.add(item.id) # 将新元素加入集合
        if len(self.queue) > self.Max:
            self.dequeue()

    def dequeue(self):
        if self.queue: # 如果队列不为空
            item = self.queue.pop(0) # 取出队首元素
            self.set.remove(item.id) # 将该元素从集合中移除
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
    data: list[dict]
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
    title: str
    lang: str
    class Config:
        json_schema_extra = {
            "example": {
                "title": "乌镇",
                "lang": "zh"
            }
        }

    @validator('lang')
    def lang_must_in_en_or_zh(cls, v):
        if v not in ['en', 'zh']:
            raise ValueError('lang must in en or zh')
        return v

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

class MetapediaPageItem(BaseModel):
    id: Optional[int]
    title: Optional[int]
    lang: Optional[int]
    redirect_from: Optional[str]

    # zh_ID: Optional[int]
    zh_title: Optional[str]
    # en_ID: Optional[int]
    en_title: Optional[str]
    zh_redirect: Optional[list[str]]
    en_redirect: Optional[list[str]]
    zh_category: Optional[list[str]]
    en_category: Optional[list[str]]


# generate a wikipedia page
class MetapediaPageGet:
    def __init__(self,title:str, lang: str):
        self.O = MetapediaPageItem()
        self.O.title = title
        self.O.lang = lang

    def _get_category(self):
        pass

    def _get_redirect_and_category(self, title: str, lang: str):
        if lang == "en":
            title = title.replace(' ','_')
        pipeline = [
            {
                "$match":{"title":title,"namespace": 0}
            },
            {
                "$lookup": {
                    'from': f'{lang}_redirect',
                    'localField': "title",
                    'foreignField': "title",
                    'as': "redirect",
                    'pipeline': [
                        {
                            '$match': {'namespace': 0}
                        }
                    ],
                }
            },
            {
                '$unwind': {
                    'path':"$redirect",
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                "$lookup": {
                    'from': f'{lang}_page',
                    'localField': "redirect._id",
                    'foreignField': "_id",
                    'as': "page"
                }
            },
            {
                '$unwind': {
                    'path':"$page",
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$group':{
                    '_id' :"$_id",
                    'redirect':{'$addToSet':"$page.title"}
                }
            },
            {
                "$lookup": {
                    'from': f'{lang}_categorylinks',
                    'localField': "_id",
                    'foreignField': "from",
                    'as': "category"
                }
            },
            {
                '$unwind': {
                    'path':"$category",
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$group':{
                    '_id' :"$_id",
                    'redirect': {'$first':"$redirect"},
                    'category':{'$addToSet':"$category.to"}
                }
            },
        ]
        cur = Database[f'{lang}_page'].aggregate(pipeline)
        try:
            doc = next(cur)
            return doc
        except Exception as e:
            print('_get_redirect_and_category fail', e, title)
            return None
        

    def _get_redirect_zh(self, title: str):
        lang = 'zh'
        pipeline = [
            {
                "$match":{"title":title,"namespace": 0}
            },
            {
                "$lookup": {
                    'from': f'{lang}_redirect',
                    'localField': "title",
                    'foreignField': "title",
                    'as': "redirect",
                    'pipeline': [
                        {
                            '$match': {'namespace': 0}
                        }
                    ],
                }
            },
            {
                '$unwind': {
                    'path':"$redirect",
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                "$lookup": {
                    'from': f'{lang}_page',
                    'localField': "redirect._id",
                    'foreignField': "_id",
                    'as': "page"
                }
            },
            {
                '$unwind': {
                    'path':"$page",
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$group':{
                    '_id' :"$_id",
                    'redirect':{'$addToSet':"$page.title"}
                }
            },
        ]
        cur = Database[f'{lang}_page'].aggregate(pipeline)
        try:
            doc = next(cur)
            return doc
        except Exception as e:
            print('_get_redirect_and_category fail', e, title)
            return None
       
    def handle(self):
        doc = Database[f'{self.O.lang}_page'].find_one({"$or":[{"title": self.O.title},{"f_title": self.O.title}], 'namespace': 0 })
        if not doc:
            return None
        self.O.id = doc['_id']
        
        if doc["is_redirect"] !=0:
            pipeline = [
                {
                    "$match":{"from":self.O.id}
                },
                {
                    "$lookup": {
                        'from': f'{self.O.lang}_page',
                        'localField': "title",
                        'foreignField': "title",
                        'as': "page",
                        'pipeline': [
                            {
                                '$match': {'namespace': 0}
                            }
                        ],
                    }
                },
                {
                    '$unwind': "$page"
                },
                {"$limit": 1}
            ]
            cur = Database[f'{self.O.lang}_redirect'].aggregate(pipeline)
            try:
                doc = next(cur)
            except:
                return self.O, 404
            if doc:
                page = doc['page']
                page["id"] = page['_id']
                self.O.id = page['_id']
                self.O.redirect_from = self.O.title
                self.O.title = page['title']

        # try to get all name
        if (self.O.lang == 'en'):
            self.O.en_title = self.O.title
            target_lang = "zh"
        elif (self.O.lang == 'zh'):
            self.O.zh_title = self.O.title
            target_lang = "en"

        # try to get zh name
        doc = Database[f'{self.O.lang}_langlinks'].find_one({'from': self.O.id,'lang': target_lang})
        if doc:
            if (self.O.lang == 'en'):
                self.O.zh_title = doc['title']
            elif (self.O.lang == 'zh'):
                self.O.en_title = doc['title']

        if self.O.en_title:
            doc = self._get_redirect_and_category(self.O.en_title,'en')
            if doc:
                self.O.en_redirect = doc['redirect']
                self.O.en_category = doc['category']

        # zh only get redirect
        if self.O.zh_title:
            doc = self._get_redirect_and_category(self.O.zh_title,'zh')
            if doc:
                self.O.zh_redirect = doc['redirect']
                self.O.zh_category = doc['category']
        
        return self.O

@router.post("/query",response_model=BaikeDemoQueryResponse)
def bake_query(item:BaikeDemoQueryRequests):
    """query in wiki
    Returns:
        wiki result
    """
    queryString = item.query
    ret = []

    response = ES8.search(index="en_page", body={
        '_source': ['title', 'id', 'images', 'redirect'],
        'query': {
            'match': {
                'title': queryString
            }
        },
        "highlight": {
            "fragment_size": 40,
            "fields": {
                "title": {}
            }
        },
        "size": 5,
    })
    for hit in response["hits"]["hits"]:
        doc = hit["_source"]
        doc['highlight'] = hit.get('highlight',{})
        doc['lang'] = 'en'
        doc['type'] = 'title'
        if 'images' in doc and len(doc['images']) > 0:
            doc['images'] = doc['images'][0]
        ret.append(doc)

    # zh query
    response = ES8.search(index="zh_page", body={
        '_source': ['title', 'id', 'zh_title', 'images', 'redirect'],
        'query': {
            'match': {
                'zh_title': queryString
            }
        },
        "highlight": {
            # "fragmenter": "span",
            "fragment_size": 40,
            "fields": {
                "zh_title": {}
            }
        },
        "size": 10,
    })
    for hit in response["hits"]["hits"]:
        doc = hit["_source"]
        doc['highlight'] = hit.get('highlight',{})
        doc['lang'] = 'zh'
        doc['type'] = 'title'
        if 'images' in doc and len(doc['images']) > 0:
            doc['images'] = doc['images'][0]
        ret.append(doc)

    return {
        'data': ret,
    }


@router.post("/page")
def query_page(item:BaikePageGetRequests):

    result = get_page(item.title, item.lang)
    if not result:
        return {'data':'','ok':False}

    Lately_Query.enqueue(result)
    return {
        'data':result,
        'ok': True
    }

@lru_cache(100)
def get_page(title, lang):
    page = MetapediaPageGet(title, lang)
    return page.handle()


@router.get("/lately_search")
def bake_page_query():
    return {
        "data": Lately_Query.queue,
        "ok": True
    }


class CategoryQueryRequests(BaseModel):
    lang: str
    title: str
    page: Optional[int]
    class Config:
        json_schema_extra = {
            "example": {
                "lang": "zh",
                "title": "TED演讲人",
            }
        }
    @validator('lang')
    def lang_must_in_en_or_zh(cls, v):
        if v not in ['en', 'zh']:
            raise ValueError('lang must in en or zh')
        return v

@router.post("/category")
def category_query(item:CategoryQueryRequests):
    # get all category
    page_limit=50
    lang = item.lang
    doc = get_category_reference(item.title, lang)
    if not doc:
        return {
            "data": "",
            "ok": False
        }

    ret = {
        'title': doc['title'],
        'in': [item['to'] for item in doc['in']],
        'out': [item['title']['title'] for item in doc['out']],
        'entity': [item['title']['title'] for item in get_category_entity(item.title, lang, 0, page_limit)],
        'entity_total': count_category_entity(item.title, lang),
        'page':0,
        'page_size': page_limit,
        'lang': item.lang
    }
    return {
        "data": ret,
        "ok": True
    }


@router.post("/category_page")
def category_query(item:CategoryQueryRequests):
    page_limit=50
    ret = {
        'entity': [item['title']['title'] for item in get_category_entity(item.title,item.lang, (item.page-1) * page_limit, page_limit)],
    }
    return {
        "data": ret,
        "ok": True
    }

    
@lru_cache(101)
def get_category_reference(title, lang):
    project = {'_id':0,'type':0, 'from':0, 'title._id':0,'title.namespace':0,'title.is_redirect':0, 'title.len':0}
    pipeline = [
        {
            '$match': {'title':title,'namespace':14}
        },
        {
            "$lookup": {
                'from': f'{lang}_categorylinks',
                'localField': "title",
                'foreignField': "to",
                'pipeline': [
                    {
                        '$match': {'type': 'subcat'}
                    },
                    {
                        "$lookup": {
                            'from': f'{lang}_page',
                            'localField': "from",
                            'foreignField': "_id",
                            'as': 'title'
                        }
                    },{
                        '$unwind':'$title'
                    },
                    {
                        '$project':project
                    }
                ],
                'as': "out"
            }
        },
        {
            "$lookup": {
                'from': f'{lang}_categorylinks',
                'localField': "_id",
                'foreignField': "from",
                'pipeline': [
                    {
                        '$match': {'type': 'subcat'}
                    },
                    {
                        '$project':project
                    }
                ],
                'as': "in"
            }
        },       
        {
            '$project': {"title":1, "in":1, "out":1}
        },
    ]
    cur = Database[f'{lang}_page'].aggregate(pipeline)
    try:
        doc = next(cur)
        return doc
    except Exception as e:
        print('_get_redirect_and_category fail')
        return None
    
@lru_cache(101)
def count_category_entity(title, lang):
    return Database[f'{lang}_categorylinks'].count_documents({'to': title, 'type': 'page'})

@lru_cache(101)
def get_category_entity(title, lang, skip, limit= 50):
    pipeline = [
        {
            '$match': {'to': title, 'type': 'page'}
        },
        {"$skip": skip}, # 跳过第一页的数据
        {"$limit": limit}, # 限制返回 10 条数据
        {
            "$lookup": {
                'from': f'{lang}_page',
                'localField': "from",
                'foreignField': "_id",
                'as': 'title'
            }
        },{
            '$unwind':'$title'
        },
        {
            '$project': {'_id': 0}
        }
    ]
    ret_docs = []
    for doc in  Database[f'{lang}_categorylinks'].aggregate(pipeline):
        ret_docs.append(doc)
    return ret_docs

class WikiPageDetailRequests(BaseModel):
    lang: str
    id: int
    class Config:
        json_schema_extra = {
            "example": {
                "lang": "zh",
                "id": 8286786,
            }
        }
    @validator('lang')
    def lang_must_in_en_or_zh(cls, v):
        if v not in ['en', 'zh']:
            raise ValueError('lang must in en or zh')
        return v

@router.post("/wiki_page_detail")
def page_one_query(item:WikiPageDetailRequests):
    try:

        result = ES8.get(index=f"{item.lang}_page", id=item.id)
        source = result.get('_source')
        if source:
            return {
                'data': source,
                'ok':True
            }
    except Exception as e:
        print('page_one_query', e, item)
        return {
            'data': "",
            'ok':False
        }

class BaiduBaikeGetRequests(BaseModel):
    title: list[str]
    baidu_title: Optional[str]
    data: Optional[dict]

@router.post("/baidu_baike")
def baidu_baike_post(item:BaiduBaikeGetRequests):
    ret = {
        'title':[],
    }
    for title in item.title:
        doc = Database['baidu_baike_page'].find_one({"title": title }, sort=[('update', -1)],)
        if not doc :
            ret['title'].append(title)
        elif doc['ok'] == True:
            ret['baidu'] = doc['data']
            return {
                'data': ret,
                'ok': True
            }
            

    return {
        'data': ret,
        'ok': False
    }

@router.put("/baidu_baike")
def baidu_baike_put(item:BaiduBaikeGetRequests):

    for title in item.title:
        Database['baidu_baike_page'].insert_one({"title": title,'ok': False,'update':datetime.datetime.now() })

    if item.baidu_title and len(item.data) > 0:
        Database['baidu_baike_page'].insert_one({"title": item.baidu_title,'ok': True,'data': item.data, 'update':datetime.datetime.now() })

    return {
        'ok': True
    }


class CategoryPathRequests(BaseModel):
    lang: str
    source: str
    target: str
    class Config:
        json_schema_extra = {
            "example": {
                "lang": "zh",
                "source": "已灭绝动物",
                "target": "原始人電影",
            }
        }
    @validator('lang')
    def lang_must_in_en_or_zh(cls, v):
        if v not in ['en', 'zh']:
            raise ValueError('lang must in en or zh')
        return v


@router.post("/category_path")
def category_distance_path_post(item:CategoryPathRequests):
    ret = get_distance_path(item.source,item.target,item.lang)
    return ret

@lru_cache(100)
def get_same_fa_category(source, target, lang):
    '''寻找两个类别的共同父类，并构建路径'''
    def parent_intersection(tx:ManagedTransaction, source,target,level):
        result = tx.run(
            '''
MATCH (c1:category{f_title: $source})
OPTIONAL MATCH (c2:category{f_title: $target})
CALL apoc.path.subgraphNodes(c1, {maxLevel:$level, relationshipFilter:'<subcat'}) YIELD node
WITH collect(node) AS nodes1, c2
CALL apoc.path.subgraphNodes(c2, {maxLevel:$level, relationshipFilter:'<subcat'}) YIELD node
WITH collect(node) AS nodes2, nodes1
WITH apoc.coll.intersection(nodes1, nodes2) AS intersection
RETURN intersection
            ''',
            source=source, target=target, level=level
        )
        result = result.single()
        if not result:
            return {}
        nodes  = result['intersection']
        return  nodes

    
    driver = get_driver()
    with driver.session(database=f"{lang}wiki") as session:
        for i in range(1, 10):
            result = session.read_transaction(parent_intersection, source,target, i)
            if result:
                break
        else:
            return None
    
    # 构建路径
    rode_results = []
    with driver.session(database=f"{lang}wiki") as session:
        for record in result:
            if record['f_title'] == source or record['f_title'] == target:
                continue
            sr = session.read_transaction(line_path, record['f_title'],source)
            dr = session.read_transaction(line_path, record['f_title'],target)
            print(sr, dr)
            sr['nodes'] = sr['nodes'][::-1]
            sr['nodes'].extend(dr['nodes'])
            sr['edges'].extend(dr['edges'])
            rode_results.append(sr)
    return rode_results


@lru_cache(100)
def shortest_path(tx:ManagedTransaction, source, target):
    '''寻找两个分类的最短路径'''
    result = tx.run(
        "MATCH (s:category {f_title: $source}), (t:category {f_title: $target}), "
        "p = shortestPath((s)-[:subcat*..15]-(t)) "
        "RETURN p",
        source=source, target=target
    )
    result = result.single()
    if not result:
        return {}
    data  = result['p']

    nodes = [node for node in data.nodes]
    edges = [(edge.start_node["title"], edge.type, edge.end_node["title"]) for edge in data.relationships]
    return  {'nodes':nodes, 'edges': edges}

def line_path(tx:ManagedTransaction, source, target):
    """寻找两个分类可能存在的层级路径"""
    result = tx.run(
        "MATCH (start:category {f_title: $source}), (end:category {f_title: $target}), p=shortestPath((start)-[:subcat*..16]->(end)) "
        "RETURN p "
        "ORDER BY length(p) ASC "
        "LIMIT 1",
        source=source, target=target
    )
    result = result.single()
    if not result:
        return {}
    data  = result['p']

    nodes = [{'title':node['title'], 'f_title': node['f_title']} for node in data.nodes]
    edges = [(edge.start_node["title"], edge.type, edge.end_node["title"]) for edge in data.relationships]
    return  {'nodes':nodes, 'edges': edges}
    
@lru_cache(100)
def get_distance_path(source, target, lang):

    driver = get_driver()
    with driver.session(database=f"{lang}wiki") as session:
        r1 = session.read_transaction(shortest_path, source,target)
        r2 = session.read_transaction(line_path, source,target)
        r3 = session.read_transaction(line_path, target, source)
    result = {
        "d1": r1,
        "d2": r2,
        "d3": r3,
    }
    # if not r2 and not r3:
    rode_results = get_same_fa_category(source, target, lang)
    result['d4s'] = rode_results
    return result