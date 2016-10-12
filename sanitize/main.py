import argparse
from collections import namedtuple

import client

EsHost = namedtuple('EsHost', 'hostname, index, user, password')


def sanitize(source, destination):
    # Setup workers
    # Get all the docs in the index size so a percentage can be displayed
    elastic_search_client = client.ElasticSearch((source.user, source.password), source.hostname,
                                                 (destination.user, destination.password), destination.hostname)
    elastic_search_client.clone_index(source_index_name=source.index,
                                      destination_index_name=destination.index)
    total_docs = elastic_search_client.get_total_docs_in_index(index_name='lcp_v2')

    # Create new index, POST, and collect stats



    # Loop to fetch results in a batch
    for result in elastic_search_client.get_docs(index_name='lcp_v2', batch_size=2):
        pass
    ## For each batch, save batch deets and status in a file
    ## For each batch, assign to a worker process to run jq
    ## For each batch, if a worker process fails to complete, re-run once more, log output
    # For each worker, parse each doc and construct a bulk API request
    # For each worker, If any docs fail, create workers/{requestid}.json
    # For each worker, if success return success response


    pass


def create_parser():
    parser = argparse.ArgumentParser(description='Create an HTTP load balanced configuration.')
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
    sanitize(source, destination)
