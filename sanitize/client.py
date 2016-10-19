import json
import logging.handlers
import time
import uuid
from collections import deque, defaultdict

import elasticsearch
from elasticsearch import helpers

import log
import util

logger = log.get_logger(__name__, logging.DEBUG)
log.add_console_handler(logger)
log.add_file_handler(logger, file_path='logs/client.log')

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.INFO)
log.add_file_handler(es_logger, file_path='logs/elasticsearch-sanitization.log')

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.setLevel(logging.INFO)
log.add_file_handler(es_tracer, file_path='logs/elasticsearch-sanitization-trace.log')

DEFAULT_SCROLL_SIZE = '10m'
DEFAULT_TIMEOUT = 100
RETRYABLE_FAILURES = [429, 500]


class ElasticSearch(object):
    AGGREGATE_BY_TYPE_QUERY = '{ \
                 "aggs": { \
                     "count_by_type": { \
                         "terms": { \
                             "field": "_type" \
                         } \
                     } \
                 } \
             }'

    def __init__(self, (user, password), host, (destination_user, destination_password), destination_host):
        self._user = user
        self._password = password
        self._host = host
        self._destination_user = destination_user
        self._destination_password = destination_password
        self._destination_host = destination_host
        self._source_client = elasticsearch.Elasticsearch(hosts=[host])
        self._destination_client = elasticsearch.Elasticsearch(hosts=[destination_host])

    def get_source_client(self):
        return elasticsearch.Elasticsearch(hosts=[self._host])

    def get_destination_client(self):
        return elasticsearch.Elasticsearch(hosts=[self._destination_host])

    def get_total_docs_in_index(self, index_name):
        """
        Return the total number of docs in the index.

        :param index_name:
        :return:
        """
        response = self._source_client.search(index=index_name, body=self.AGGREGATE_BY_TYPE_QUERY, search_type='count',
                                              request_timeout=DEFAULT_TIMEOUT)
        logger.debug(response)
        try:
            return response['hits']['total']
        except KeyError:
            logger.error('Unable to get total docs in index.')
            raise

    def get_docs_types_in_index(self, index_name):
        """
        Return a list of docs types in the index.

        :param index_name:
        :return:
        """
        response = self._source_client.search(index=index_name, body=self.AGGREGATE_BY_TYPE_QUERY, search_type='count',
                                              request_timeout=DEFAULT_TIMEOUT)
        logger.debug(response)
        try:
            return [item['key'] for item in response['aggregations']['count_by_type']['buckets']]
        except KeyError:
            logger.error('Unable to get doc types in index.')
            raise

    def get_docs(self, index_name, batch_size):
        """
        Return a batch of docs using the scroll scan api to avoid loading all docs.

        :param batch_size:
        :return:
        """
        match_all_query = '{ "query": {"matchAll":{}} }'
        doc_types = self.get_docs_types_in_index(index_name)
        response = self._source_client.search(index=index_name, body=match_all_query, scroll=DEFAULT_SCROLL_SIZE,
                                              doc_type=doc_types, size=batch_size, search_type='scan',
                                              request_timeout=DEFAULT_TIMEOUT)
        scroll_id = response.get('_scroll_id')
        logger.debug('Scrollid {}'.format(scroll_id))
        response = self._source_client.scroll(scroll_id, scroll=DEFAULT_SCROLL_SIZE, request_timeout=DEFAULT_TIMEOUT)
        return response['hits']['hits'], response.get('_scroll_id')

    def get_scroll(self, scroll_id):
        """
        Get the results of a scroll id.

        :param scroll_id:
        :param scroll:
        :return: The documents in the scroll and the next scroll id
        """
        response = self._source_client.scroll(scroll_id, scroll=DEFAULT_SCROLL_SIZE, request_timeout=DEFAULT_TIMEOUT)
        return response['hits']['hits'], response.get('_scroll_id')

    def delete_index(self, index_name):
        """
        Delete an index

        :param source_index_name:
        :return:
        """
        try:
            self._destination_client.indices.delete(index=index_name)
        except elasticsearch.exceptions.NotFoundError:
            logger.debug('Index does not exist on host.')

    def clone_index(self, source_index_name, destination_index_name):
        """
        Clone an existing index into an Elasticsearch host.

        :param source_index_name:
        :param destination_index_name:
        :param delete_before_create:
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
                                                           body=destination_index_body,
                                                           request_timeout=DEFAULT_TIMEOUT)
        except elasticsearch.exceptions.RequestError as e:
            if 'IndexAlreadyExistsException' not in e.error:
                raise e
            logger.debug('Destination index already exists.')

    def get_index(self, index_name):
        return self._source_client.indices.get(index=index_name,
                                               feature=['_settings', '_mappings'],
                                               flat_settings=True,
                                               request_timeout=DEFAULT_TIMEOUT)

    def bulk_insert(self, results, record_failures=True):
        """
        Insert documents in parallel using the bulk api.
        :param results:
        :param record_failures:
        :return: total successful docs, total failed docs
        """
        # logger.debug('About to bulk insert.')
        responses = []
        try:
            with util.Timer() as t:
                responses = deque(
                    helpers.parallel_bulk(client=self.get_destination_client(), actions=results, chunk_size=100,
                                          thread_count=20, raise_on_error=False, request_timeout=DEFAULT_TIMEOUT))
            logger.debug("Bulk insert time elapsed {} ".format(t.secs))
        except Exception as e:
            logger.exception(e)

        total_docs_processed = len(responses)
        failures, failure_breakup = _extract_and_total_failures(responses)
        total_failed_docs = len(failures)
        total_successful_docs = total_docs_processed - total_failed_docs

        if set(RETRYABLE_FAILURES).intersection(set(failure_breakup.keys())):
            retry_results = [item for item in responses if item[1]['create']['status'] in RETRYABLE_FAILURES]
            time.sleep(5)  # give ES a breather
            logger.debug('Retrying failures.')
            total_retry_success_docs, total_retry_failed_docs, failures = self.bulk_insert(retry_results,
                                                                                           record_failures=False)
            total_successful_docs += total_retry_success_docs
            total_failed_docs = total_retry_failed_docs

        if record_failures:
            _write_failures(failures)

        return total_successful_docs, total_failed_docs, failures


def _write_failures(failures):
    with open('logs/failures/{}.json'.format(uuid.uuid4()), 'w+') as f:
        f.write(json.dumps(failures))


def _extract_and_total_failures(responses):
    failures = []
    failure_breakup = defaultdict(int)
    for response in responses:
        if response[1]['create']['status'] != 201:
            failures.append(response)
            failure_breakup[response[1]['create']['status']] += 1
            if response[1]['create']['status'] not in [429, 500]:
                logger.error(
                    'Unknown response received - doc id {} http response {}'.format(response[1]['create']['_id'],
                                                                                    response[1]['create'][
                                                                                        'status']))
                logger.error(response)
    for key, value in failure_breakup.iteritems():
        logger.debug('Failure break up {} {}'.format(key, value))
    return failures, failure_breakup
