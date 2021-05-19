import logging
import os
import json
import time
from pprint import pprint


from elasticsearch import helpers

from searchengine.connect import es

_path_to_data = os.path.dirname(os.path.abspath(__file__)) + '/news_data.json'


def create_index(name):
    created = False
    settings = {
        "settings": {
            'index': {
                'analysis': {
                    'analyzer': {
                        'default': {
                            'type': 'parsi'
                        }
                    }
                }
            }
        },
        'mappings': {
            'properties': {
                'Body': {
                    'type': 'text',
                    'analyzer': 'parsi'
                },
                'Title': {
                    'type': 'text',
                    'analyzer': 'parsi'
                },
                'Cat_fa': {
                    'type': 'text',
                    'analyzer': 'parsi'
                },

            }
        }
    }

    try:
        if not es.indices.exists(name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            print(es.indices.create(index=name, ignore=400, body=settings))

            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def add_docs(index_name):
    f = open(_path_to_data, 'r')
    bulk_data = []
    raw_data_list = list(json.loads(f.read()))
    i = 0
    for raw_data in raw_data_list:
        i += 1
        op_dict = {
            '_index': index_name,
            '_source': raw_data
        }
        bulk_data.append(op_dict)
        # print(raw_data['Cat_fa'])
        if i % 100 == 0:
            logging.info(i)
        if i % 100 == 0:
            logging.info('bulk to elastic search')
            # es.bulk(index=index_name, body=op_dict)
            helpers.bulk(client=es, actions=bulk_data)
            bulk_data.clear()
            break
        if i == 10000:
            return


def delete_index(name):
    if es.indices.exists(name):
        print("deleting '%s' index..." % (name))
        res = es.indices.delete(index=name)
        print(" response: '%s'" % (res))


def search(query, index_name, title):
    search_param2 = {
        "query": {
            "match": {
                "doc.content": {
                    "query": query,
                    "analyzer": "my_custom_analyzer"
                },
                "doc.title": {
                    "query": title
                }
            }
        }
    }
    print(es.search(body=search_param2, index=index_name))


if __name__ == '__main__':
    name = 'examind'
    delete_index(name)
    create_index(name)

    add_docs(name)
    res = es.search(index=name, size=100, body={
        "query": {
            "query_string": {
                "query": '(سلامت)^1 (سیاسی)^2 (اجتماعی)^4 (ورزشی)^3',
                "analyzer": "parsi",
                "fields": ['Cat_fa']
            }
        }
    })
    print(len(res['hits']['hits']))
