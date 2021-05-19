from decouple import config

_HOST = config('ELASTICSEARCH_HOST', default='localhost')
_PORT = config('ELASTICSEARCH_PORT', default=9200, cast=int)

connection_config = {
    'host': _HOST,
    'port': _PORT
}



