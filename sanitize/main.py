import argparse
import json
import logging
import os
import shutil
import subprocess
import threading
from collections import namedtuple

import client
import log
import util

logger = log.get_logger(__name__, logging.DEBUG)
log.add_console_handler(logger)
log.add_file_handler(logger, file_path='logs/main.log')

EsHost = namedtuple('EsHost', 'hostname, index, user, password')
max_threads = 0
num_threads = 0
success_count, failure_count, processed_docs_count = 0.0, 0.0, 0.0


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
    # json_me = pyjq.first('''def mask(f):
    #     with_entries(
    #         if .key |in({"cardNumber":null,"cardName":null}) then
    #             if .value | type == "number" then
    #                 .value |= 0
    #             elif .value | type == "string" then
    #                 .value |= "***"
    #             else
    #                 .
    #             end
    #         else
    #             .
    #         end
    #     );
    # def walk(f):
    #   . as $in
    #   | if type == "object" then
    #       reduce keys[] as $key
    #         ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
    #   elif type == "array" then map( walk(f) ) | f
    #   else f
    #   end;
    # walk(if type == "object" then
    #     del (._attachments) | mask(.)
    # else
    #     .
    # end)''', data_dict)
    # return json_me

    json_data = json.dumps(data_dict)
    p1 = subprocess.Popen('/app/jsonymous --config /app/config'.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    output = p1.communicate(json_data)
    return json.loads(output[0])


def _sanitize_and_insert(results, client, index):
    results = _prepare_data(index, results)
    results = _sanitize(results)
    return client.bulk_insert(results)


def _prepare_data(index, results):
    for result in results:
        result.pop('_score')
        result['doc'] = result.pop('_source')  # rename dict key
        result['_op_type'] = 'create'
        result['_index'] = index
    return results


def _process(results, client, index, lock):
    _increment_num_threads(lock)
    total_success_docs, total_failed_docs, _ = _sanitize_and_insert(results, client, index)
    _update_stats(total_failed_docs, total_success_docs, lock)
    _decrement_num_threads(lock)


def _increment_num_threads(lock):
    global num_threads
    with lock:
        num_threads += 1


def _decrement_num_threads(lock):
    global num_threads
    with lock:
        num_threads -= 1


def _update_stats(total_failed_docs, total_success_docs, lock):
    global success_count, failure_count, processed_docs_count
    with lock:
        success_count += total_success_docs
        failure_count += total_failed_docs

        processed_docs_count += (total_success_docs + total_failed_docs)
        logger.debug('Processed doc count {}'.format(processed_docs_count))


def sanitize(source, destination, reset_destination=False):
    elastic_search_client = client.ElasticSearch((source.user, source.password), source.hostname,
                                                 (destination.user, destination.password), destination.hostname)
    make_destination_index(destination.index, elastic_search_client, source.index, reset_destination)
    total_docs = elastic_search_client.get_total_docs_in_index(index_name='lcp_v2')
    logger.debug("total_docs {} ".format(total_docs))
    results, next_scroll_id = elastic_search_client.get_docs(index_name='lcp_v2', batch_size=1000)

    lock = threading.RLock()
    global num_threads
    while len(threading.enumerate()) > 0:
        while (num_threads < max_threads) and len(results) > 0:
            t = threading.Thread(target=_process, args=(results, elastic_search_client, destination.index, lock))
            t.start()
            # _process(results, elastic_search_client, destination.index, lock)
            with util.Timer() as t:
                results, next_scroll_id = elastic_search_client.get_scroll(scroll_id=next_scroll_id)
            logger.debug("Get data time elapsed {} ".format(t.secs))
            logger.debug(
                "# of active threads-{}, len(results)-{}, success-{}, failures-{}, % Completion-{}".format(num_threads,
                                                                                                           len(results),
                                                                                                           success_count,
                                                                                                           failure_count,
                                                                                                           (
                                                                                                           processed_docs_count / total_docs) * 100))

    _wait_for_threads_to_complete()
    global success_count, failure_count, processed_docs_count
    logger.debug('Final results: success_count {}, failure_count {}, processed_docs_count {}, num_threads {}'.format(
        success_count, failure_count, processed_docs_count, num_threads))
    pass


def make_destination_index(destination_index, elastic_search_client, source_index, reset_destination):
    if reset_destination:
        elastic_search_client.delete_index(index_name=destination_index)
    elastic_search_client.clone_index(source_index_name=source_index, destination_index_name=destination_index)


def _wait_for_threads_to_complete():
    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is not main_thread:
            t.join()


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
    parser.add_argument('-rd', '--reset_destination',
                        help='Reset the Destination Elasticsearch index. If it exists it will be deleted first.',
                        default=False, action="store_true")
    parser.add_argument('-n', '--max_threads', help='Maximum number of threads of execution.', default=2)
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    source = EsHost(hostname=args.source, index=args.source_index, user=args.user, password=args.password)
    destination = EsHost(hostname=args.destination, index=args.destination_index, user=args.destination_user,
                         password=args.destination_password)
    max_threads = args.max_threads
    reset_logs()
    sanitize(source, destination, reset_destination=args.reset_destination)
