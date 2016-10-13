from collections import deque
import json
import logging.handlers
import uuid

import elasticsearch
from elasticsearch import helpers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.INFO)
es_logger_handler = logging.handlers.RotatingFileHandler('logs/elasticsearch-sanitization.log',
                                                         maxBytes=0.5 * 10 ** 9,
                                                         backupCount=3)
es_logger.addHandler(es_logger_handler)

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.setLevel(logging.DEBUG)
es_tracer_handler = logging.handlers.RotatingFileHandler('logs/elasticsearch-sanitization-trace.log',
                                                         maxBytes=0.5 * 10 ** 9,
                                                         backupCount=3)
es_tracer.addHandler(es_tracer_handler)

DEFAULT_SCROLL_SIZE = '10m'


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

    def get_docs(self, batch_size):
        """
        Return a batch of docs using the scroll scan api to avoid loading all docs.

        :param batch_size:
        :return:
        """
        response = self._source_client.search(body='{ "query": {"matchAll":{}} }', scroll=DEFAULT_SCROLL_SIZE,
                                              size=batch_size, search_type='scan')
        scroll_id = response.get('_scroll_id')
        response = self._source_client.scroll(scroll_id, scroll=DEFAULT_SCROLL_SIZE)
        return response['hits']['hits'], response.get('_scroll_id')

    def get_scroll(self, scroll_id):
        """
        Get the results of a scroll id.

        :param scroll_id:
        :param scroll:
        :return: The documents in the scroll and the next scroll id
        """
        response = self._source_client.scroll(scroll_id, scroll=DEFAULT_SCROLL_SIZE)
        return response['hits']['hits'], response.get('_scroll_id')

    def clone_index(self, source_index_name, destination_index_name):
        """
        Clone an existing index into an Elasticsearch host.

        :param source_index_name:
        :param destination_index_name:
        :return:
        """

        def index_body(index_name, index_properties_response):
            mappings = index_properties_response[index_name]['mappings']
            settings = index_properties_response[index_name]['settings']
            body = {
                "settings": {
                    "index": {
                        "number_of_shards": settings['index.number_of_shards'],
                        "number_of_replicas": settings['index.number_of_replicas']
                    }
                },
                "mappings": mappings
            }
            return json.dumps(body)

        source_index_response = self.get_index(index_name=source_index_name)
        destination_index_body = index_body(source_index_name, source_index_response)

        try:
            return self._destination_client.indices.create(index=destination_index_name,
                                                           body=destination_index_body)
        except elasticsearch.exceptions.RequestError as e:
            if 'IndexAlreadyExistsException' not in e.error:
                raise e
            logger.debug('Destination index already exists.')

    def get_index(self, index_name):
        return self._source_client.indices.get(index=index_name,
                                               feature=['_settings', '_mappings'],
                                               flat_settings=True)

    def bulk_insert(self, results, record_failures=True):
        """
        Insert documents in parallel using the bulk api.
        :param results:
        :param record_failures:
        :return: total successful docs, total failed docs
        """
        logger.debug('About to bulk insert.')
        responses = deque(helpers.parallel_bulk(client=self._destination_client, actions=results, chunk_size=10,
                                                thread_count=5, raise_on_error=False))
        total_docs_processed = len(responses)
        total_failed_docs = 0
        if record_failures:
            total_failed_docs = _write_failures(responses)
        return total_docs_processed - total_failed_docs, total_failed_docs


def _write_failures(responses):
    failures = [response[1]['create']['_id'] for response in responses if response[1]['create']['status'] != '201']
    total_failed_docs = len(failures)
    with open('logs/failures/{}.json'.format(uuid.uuid4()), 'w+') as f:
        f.write(json.dumps(failures))
    return total_failed_docs
