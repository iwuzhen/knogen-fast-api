from faker import Faker
from fastapi import APIRouter
from functools import lru_cache
import pymongo
from pydantic import BaseModel
import math
from collections import OrderedDict

Database = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.227").get_database("openalex")

router = APIRouter(
    prefix="/openalex",
    tags=["openalex"],
    responses={404: {"description": "Not found"}},
)

concept_id_name_map = {
  15744967: 'Psychology',
  17744445: 'Political science',
  33923547: 'Mathematics',
  39432304: 'Environmental science',
  41008148: 'Computer science',
  71924100: 'Medicine',
  86803240: 'Biology',
  95457728: 'History',
  121332964: 'Physics',
  127313418: 'Geology',
  127413603: 'Engineering',
  138885662: 'Philosophy',
  142362112: 'Art',
  144024400: 'Sociology',
  144133560: 'Business',
  162324750: 'Economics',
  185592680: 'Chemistry',
  192562407: 'Materials science',
  205649164: 'Geography'
}

faker = Faker()

class OpenalexCountryGoogleDistanceRequests(BaseModel):
    countryA: str
    countryB: list[str]
    class Config:
        json_schema_extra = {
            "example": {
                "countryA": "US",
                "countryB": ["UK","CN"]
            }
        }

class OpenalexCountrySubjectGoogleDistanceRequests(BaseModel):
    countryA: str
    countryB: list[str]
    subjectA: int
    subjectB: int
    class Config:
        json_schema_extra = {
            "example": {
                "countryA": "US",
                "countryB": ["UK","CN"],
                "subjectA": 33923547,
                "subjectB": 33923547, 
            }
        }

class OpenalexCountryWorksCountRequests(BaseModel):
    countries: list[str]
    typenames: list[str]
    class Config:
        json_schema_extra = {
            "example": {
                "countries": ["US","UK","CN","UNKNOW"],
                "typenames": ["standard","report-series", "reference-entry"]
            }
        }
    
class OpenalexForcesCountrySubjectRequests(BaseModel):
    countries: list[str]
    subjects: list[int]
    class Config:
        json_schema_extra = {
            "example": {
                "countries": ["US","UK","CN","UNKNOW"],
                "subjects": [123,12346],
            }
        }
    
class OpenalexEchartsResponse(BaseModel):
    data: list[list[float|str]]
    dimensions: list[str]
    class Config:
        json_schema_extra = {
            "example": {
                "data": [[faker.pyfloat(min_value=0,max_value=1) for _ in range(5)] ]* 20,
                "dimensions":["year","a","b","c","d"]
            }
        }

class OpenalexEchartsForceResponse(BaseModel):
    links: list[list[dict]]
    nodes: list[dict]
    years: list[str]
    class Config:
        json_schema_extra = {
            "example": {
                "links": [],
                "nodes":[],
                "years":[]
            }
        }

@lru_cache(1000)
def get_country_distance(a:str,b:str,year_start:int,year_end:int) -> list[float]:
    names = [a,b]
    names.sort()
    ret = ['-'] * (year_end-year_start+1)
    for doc in Database.country_google_distance.find({'a':names[0],'b':names[1],'year':{'$gte':year_start,'$lte':year_end}}).sort('year',pymongo.ASCENDING):
        if not math.isnan(doc['d_total']) and not math.isinf(doc['d_total']):
            ret[doc['year']-year_start] = round(doc['d_total'],4) if doc['d_total'] < 1 else 1
    return ret


@lru_cache(1000)
def get_country_distance_v2(a:str,b:str,year_start:int,year_end:int) -> list[float]:
    names = [a,b]
    names.sort()
    doc = Database.country_google_distance_v2.find_one({'a':names[0],'b':names[1]})

    if not doc:
        print("country_distance_v query fail",names)
        return []

    # slip slice
    if year_start < doc['start_year'] or year_end > doc['end_year']:
        print("time out of range", year_start, year_end)
        return []

    ret_list = []
    for item in  doc['d_total'][year_start-doc['start_year']: len(doc['d_total']) + doc['end_year'] - year_end + 1]:
        if not math.isnan(item) and not math.isinf(item):
            ret_list.append(round(item,4) if item < 1 else 1)
        else:
            ret_list.append('-')
    return ret_list

@router.post("/googledistance",response_model=OpenalexEchartsResponse)
def openalex_google_distance(item:OpenalexCountryGoogleDistanceRequests):
    """query distance by country a,b
    Returns:
        echarts dataset
    """
    # ret = OpenalexEchartsResponse()
    year_start=1960
    year_end=2022

    ret,dimensions = [],[]
    for name in item.countryB:
        if name == item.countryA:
            continue
        dimensions.append(name)
        ret.append(get_country_distance_v2(item.countryA,name,year_start,year_end))

    # sort dimensions by last value
    try:
        dimensions,ret = zip(*sorted(zip(dimensions, ret),key=lambda x:x[1][-1],reverse=True))
    except Exception as e:
        print(dimensions,ret, e)
    ret = [[v for v in range(year_start,year_end+1)],*ret]
    dimensions = ['year',*dimensions]

    return {
        'data': list(zip(*ret)),
        'dimensions': dimensions
    }

@lru_cache(1000)
def get_country_subject_distance(a:str,b:str,subjectIDa:int, subjectIDb:int,year_start:int,year_end:int) -> list[float]:
    names = [a,b]
    names.sort()
    doc = Database.country_google_distance_concept_v2.find_one({'a':names[0],'b':names[1],'ac':subjectIDa,'bc':subjectIDb})

    if not doc:
        print("country_distance_v query fail",names,subjectIDa,subjectIDb)
        return []

    # slip slice
    if year_start < doc['start_year'] or year_end > doc['end_year']:
        print("time out of range", year_start, year_end)
        return []

    ret_list = []
    for item in  doc['d_total'][year_start-doc['start_year']: len(doc['d_total']) - doc['end_year'] + year_end ]:
        if not math.isnan(item) and not math.isinf(item):
            ret_list.append(round(item,4) if item < 1 else 1)
        else:
            ret_list.append('-')
    return ret_list

@router.post("/googledistance_subject",response_model=OpenalexEchartsResponse)
def openalex_google_distance_subject(item:OpenalexCountrySubjectGoogleDistanceRequests):
    """query distance by country a,b
    Returns:
        echarts dataset
    """
    # ret = OpenalexEchartsResponse()
    year_start=1960
    year_end=2022

    ret,dimensions = [],[]
    for name in item.countryB:
        if name == item.countryA:
            continue
        dimensions.append(name)
        ret.append(get_country_subject_distance(item.countryA,name,item.subjectA,item.subjectB,year_start,year_end))

    # sort dimensions by last value
    try:
        dimensions,ret = zip(*sorted(zip(dimensions, ret),key=lambda x:x[1][-1],reverse=True))
    except:
        print(dimensions,ret)
    ret = [[v for v in range(year_start,year_end+1)],*ret]
    dimensions = ['year',*dimensions]

    return {
        'data': list(zip(*ret)),
        'dimensions': dimensions
    }

@lru_cache(1000)
def get_country_works_count(country:str|None,type_names:tuple[str],year_start:int,year_end:int) -> list[float]:
    ret = [0] * (year_end-year_start+1)
    for doc in Database.works_count_by_country.find({'n':country,'t':{'$in':type_names},'y':{'$gte':year_start,'$lte':year_end}}).sort('y',pymongo.ASCENDING):
        ret[doc['y']-year_start] += doc['c']
    return ret

@router.post("/countryworkscount",response_model=OpenalexEchartsResponse)
def openalex_country_count(item:OpenalexCountryWorksCountRequests):
    """ empty country name is `UNKNOW`
        empty type name is `UNKNOW`
    Returns:
        echarts dataset
    """
    for i, word in enumerate(item.typenames):
        if word == 'UNKNOW':
            item.typenames[i] = ''
    typeNames = tuple(item.typenames)

    year_start=1920
    year_end=2022
    ret,dimensions = [],[]
    for country_name in set(item.countries):
        dimensions.append(country_name)
        if country_name == "UNKNOW":
            country_name = None
        ret.append(get_country_works_count(country_name,typeNames,year_start,year_end))

    # sort dimensions by last value
    dimensions,ret = zip(*sorted(zip(dimensions, ret),key=lambda x:x[1][-1],reverse=True))
    ret = [[v for v in range(year_start,year_end+1)],*ret]
    dimensions = ['year',*dimensions]

    return {
        'data': list(zip(*ret)),
        'dimensions': dimensions
    }


@router.post("/force_distance_country_subject",response_model=OpenalexEchartsForceResponse)
def openalex_force_distance(item:OpenalexForcesCountrySubjectRequests):
    """query distance by country a,b
    Returns:
        echarts dataset
    """
    # ret = OpenalexEchartsResponse()
    year_start=2000
    year_end=2021

    nodeCache = OrderedDict()
    linkCache = [[] for _ in range(year_end-year_start+1)]
    nodeSeq = 0

    ret,dimensions = [],[]
    for name_1 in item.countries:
        for subject_1 in item.subjects:
            key_1 = f'{name_1}-{concept_id_name_map[subject_1]}'
            if key_1 not in nodeCache:
                nodeCache[key_1] = nodeSeq
                nodeSeq+=1
            for name_2 in item.countries:
                for subject_2 in item.subjects:
                    key_2 = f'{name_2}-{concept_id_name_map[subject_2]}'
                    if key_2 not in nodeCache:
                        nodeCache[key_2] = nodeSeq
                        nodeSeq+=1
                    if key_1 == key_2:
                        continue

                    ret = get_country_subject_distance(name_1,name_2,subject_1,subject_2,year_start,year_end)
                    for i in range(len(ret)):
                        if ret[i] == "-":
                            continue
                        else:
                            linkCache[i].append({
                                "source": nodeCache[key_1],
                                "target":  nodeCache[key_2],
                                "value": ret[i],
                            })

    # sort dimensions by last value
    try:
        dimensions,ret = zip(*sorted(zip(dimensions, ret),key=lambda x:x[1][-1],reverse=True))
    except:
        print(dimensions,ret)
    ret = [[v for v in range(year_start,year_end+1)],*ret]
    dimensions = ['year',*dimensions]

    return {
        'links': linkCache,
        'nodes': [{"name": name, "id":_id} for name,_id in nodeCache.items()],
        'years' : [str(year) for year in range(year_start, year_end+1)]
    }
