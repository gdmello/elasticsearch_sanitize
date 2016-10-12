import json
import logging
import logging.handlers
import elasticsearch
from elasticsearch import helpers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.INFO)
es_logger_handler = logging.handlers.RotatingFileHandler('top-camps-base.log',
                                                         maxBytes=0.5 * 10 ** 9,
                                                         backupCount=3)
es_logger.addHandler(es_logger_handler)

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.setLevel(logging.DEBUG)
es_tracer_handler = logging.handlers.RotatingFileHandler('top-camps-full.log',
                                                         maxBytes=0.5 * 10 ** 9,
                                                         backupCount=3)
es_tracer.addHandler(es_tracer_handler)


class ElasticSearch(object):
    def __init__(self, (user, password), host, (destination_user, destination_password), destination_host):
        self._user = user
        self._password = password
        self._host = host
        self._destination_user = destination_user
        self._destination_password = destination_password
        self._destination_host = destination_host
        self._source_client = elasticsearch.Elasticsearch(hosts=[host])
        self._destination_client = elasticsearch.Elasticsearch(hosts=[destination_host])

    def get_total_docs_in_index(self, index_name):
        """
        Return the total number of docs in the index based on the 'primaries'.
        This is to not include replicated docs in the count.

        :param index_name:
        :return:
        """
        response = self._source_client.indices.stats(index=[index_name], metric='docs')
        logger.debug(response)
        try:
            return response['indices'][index_name]['primaries']['docs']['count']
        except KeyError:
            logger.error('Unable to get total docs in index.')
            raise

    def get_docs(self, index_name, batch_size):
        response = helpers.scan(client=self._source_client,
                                query='{ "query": {"matchAll":{}} }',
                                scroll='2h',
                                preserve_order=True,
                                size=batch_size)
        return response

    def clone_index(self, source_index_name, destination_index_name):
        """
        Clone an existing index into an Elasticsearch host.

        :param source_index_name:
        :param destination_index_name:
        :return:
        """
        source_index_response = self._source_client.indices.get(index=source_index_name,
                                                                feature=['_settings', '_mappings'],
                                                                flat_settings=True)
        mappings = source_index_response[source_index_name]['mappings']
        settings = source_index_response[source_index_name]['settings']
        destination_index_body = {
            "settings": {
                "index": {
                    "number_of_shards": settings['index.number_of_shards'],
                    "number_of_replicas": settings['index.number_of_replicas']
                }
            },
            "mappings": mappings
        }
        response = self._destination_client.indices.create(index=destination_index_name,
                                                body=json.dumps(destination_index_body))
        print response
