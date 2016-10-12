import argparse

import client


def sanitize((user, password), (destination_user, destination_password), source, destination):
    # Setup workers
    # Loop to fetch results in a batch
    client.ElasticSearch(('admin', 'password')).get_total_docs_in_index(index_name='lcp_v2')
    # Get all the docs in the index size so a percentage can be displayed

    ## For each batch, save batch deets and status in a file
    ## For each batch, assign to a worker process to run jq
    ## For each batch, if a worker process fails to complete, re-run once more, log output
    # For each worker, parse each doc and construct a bulk API request
    # For each worker, create index, POST, and collect stats
    # For each worker, If any docs fail, create workers/{requestid}.json
    # For each worker, if success return success response


    pass


def create_parser():
    parser = argparse.ArgumentParser(description='Create an HTTP load balanced configuration.')
    parser.add_argument('-u', '--user', help='Source Elasticsearch admin username.', required=True)
    parser.add_argument('-p', '--password', help='Source Elasticsearch admin password.', required=True)
    parser.add_argument('-s', '--source', help='Source Elasticsearch host.',
                        required=True)
    parser.add_argument('-du', '--destination_user', help='Destination Elasticsearch admin username.', required=True)
    parser.add_argument('-dp', '--destination_password', help='Destination Elasticsearch admin password.',
                        required=True)
    parser.add_argument('-s', '--destination', help='Destination Elasticsearch host in which the new sanitized index will be created.',
                        required=True)
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    source_credentials = (args.user, args.password)
    destination_credentials = (args.destination_user, args.destination_password)
    source_credentials = ('user', 'password')
    destination_credentials = ('destination_user', 'destination_password')
    source=args.source
    destination=args.destination
    sanitize(source_credentials, destination_credentials, source, destination)
