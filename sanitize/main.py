import os
import shutil
import argparse
from collections import namedtuple

import client

EsHost = namedtuple('EsHost', 'hostname, index, user, password')


def reset_logs():
    if os.path.exists('logs/failures'):
        shutil.rmtree('logs/failures')
    os.makedirs('logs/failures')


def _sanitize(data_dict):
    """
    Perform sanitization on the doc using a custom implementation. Left as an exercise to the Reader!
    Can use a tool like JQ to perform streaming sanitization of data for PCI/ Compliance/ Privacy reasons.
    :param data_dict:
    :return:
    """
    return data_dict


def _sanitize_and_insert(results, client, index):
    bulk_insert = []
    for result in results:
        result.pop('_score')
        # sanitize result.pop('_source'), below, before inserting
        # leaving sanitization as an implementation detail
        result['doc'] = _sanitize(result.pop('_source'))  # rename dict key
        result['_op_type'] = 'create'
        result['_index'] = index
        bulk_insert.append(result)
    return client.bulk_insert(results)


def sanitize(source, destination):
    elastic_search_client = client.ElasticSearch((source.user, source.password), source.hostname,
                                                 (destination.user, destination.password), destination.hostname)
    elastic_search_client.clone_index(source_index_name=source.index,
                                      destination_index_name=destination.index)
    total_docs = elastic_search_client.get_total_docs_in_index(index_name='lcp_v2')
    success_docs, failed_docs, processed_docs = 0, 0, 0
    results, next_scroll_id = elastic_search_client.get_docs(batch_size=5)
    total_success_docs, total_failed_docs = _sanitize_and_insert(results, elastic_search_client, destination.index)
    processed_docs += (total_success_docs + total_failed_docs)
    failed_docs += (failed_docs + total_failed_docs)
    success_docs += (success_docs + total_success_docs)
    ## For each batch, save batch deets and status in a file
    ## For each batch, assign to a worker process to run jq
    ## For each batch, if a worker process fails to complete, re-run once more, log output


    pass


def create_parser():
    parser = argparse.ArgumentParser(description='Sanitize an Elasticsearch index into a new index.')
    parser.add_argument('-u', '--user', help='Source Elasticsearch admin username.', required=True)
    parser.add_argument('-p', '--password', help='Source Elasticsearch admin password.', required=True)
    parser.add_argument('-s', '--source', help='Source Elasticsearch host.', required=True)
    parser.add_argument('-si', '--source_index', help='Source Elasticsearch index.', default='lcp_v2')
    parser.add_argument('-du', '--destination_user', help='Destination Elasticsearch admin username.', required=True)
    parser.add_argument('-dp', '--destination_password', help='Destination Elasticsearch admin password.',
                        required=True)
    parser.add_argument('-di', '--destination_index', help='Destination Elasticsearch index.', default='lcp_v2_copy')
    parser.add_argument('-d', '--destination',
                        help='Destination Elasticsearch host in which the new sanitized index will be created.',
                        required=True)
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    source = EsHost(hostname=args.source, index=args.source_index, user=args.user, password=args.password)
    destination = EsHost(hostname=args.destination, index=args.destination_index, user=args.destination_user,
                         password=args.destination_password)
    reset_logs()
    sanitize(source, destination)
