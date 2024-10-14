import pymongo

# 统计 country 的不同类别的文章count

# type cal
collection_source = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.229").openalex.work_type
Type_map = {}
for doc in collection_source.find():
    Type_map[doc['_id']] = doc['name']

collection_source = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.229").openalex.works

collection_dest = pymongo.MongoClient("mongodb://knogen:knogen@192.168.1.227").openalex.works_count_by_country

countries = [None,"US", "CN", "GB", "DE", "JP", "FR", "IN", "CA", "BR", "IT", "AU", "ES", "KR", "RU", "NL", "ID", "PL", "IR", "SE", "CH", "TR", "TW", "BE", "MX", "IL", "DK", "AT", "FI", "ZA", "PT"]
# countries = [None,]
year_start = 1800
year_end = 2024

for cname in countries:
    for typeID,typeName in Type_map.items():
        for year in range(year_start, year_end+1):
            nv = collection_source.count_documents({'y':year,'t':typeID,'c':cname})
            collection_dest.insert_one({
                '_id': f'{cname}-{year}-{typeName}',
                'n':cname,
                't':typeName,
                'y':year,
                'c':nv
            })