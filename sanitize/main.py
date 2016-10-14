import json
import os
import shutil
import subprocess
import argparse
from collections import namedtuple
import threading

import pyjq, logging

import client

EsHost = namedtuple('EsHost', 'hostname, index, user, password')

# logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
formatter = logging.Formatter(LOG_FORMAT)
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)


def reset_logs():
    if os.path.exists('logs/failures'):
        shutil.rmtree('logs/failures')
    os.makedirs('logs/failures')


import time

success_count, failure_count, processed_docs_count = 0, 0, 0


def _sanitize(data_dict):
    """
    Perform sanitization on the doc using a custom implementation. Left as an exercise to the Reader!
    Can use a tool like JQ to perform streaming sanitization of data for PCI/ Compliance/ Privacy reasons.
    :param data_dict:
    :return:
    """
    # print "="*100
    json_me = pyjq.first('''def mask(f):
        with_entries(
            if .key |in({"cardNumber":null,"cardName":null}) then
                if .value | type == "number" then
                    .value |= 0
                elif .value | type == "string" then
                    .value |= "***"
                else
                    .
                end
            else
                .
            end
        );
    def walk(f):
      . as $in
      | if type == "object" then
          reduce keys[] as $key
            ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
      elif type == "array" then map( walk(f) ) | f
      else f
      end;
    walk(if type == "object" then
        del (._attachments) | mask(.)
    else
        .
    end)''', data_dict)
    # print '>>>>>>>>>>>>>>>>>>>>>> {}'.format(type(json_me))
    # logger.debug(data_dict)
    # print("About to` jsonify")

    # json_data = json.dumps(data_dict)
    # print("jsonify --- {}".format(time.clock()))
    # p1 = subprocess.Popen('/home/gavin.dmello/new_wk_spc/jsonymous/jsonymous --config /home/gavin.dmello/new_wk_spc/jsonymous/config'.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # output = p1.communicate(json_data)
    # print("jsonymous --- {}".format(time.clock()))
    # print('--- len(dict) {}'.format(len(output[0])))
    # print(json_me)
    return json_me


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


MAX_THREADS = 3
num_threads = 0


def _process(results, client, index, lock):
    _increment_num_threads(lock)
    total_success_docs, total_failed_docs = _sanitize_and_insert(results, client, index)
    _update_stats(total_failed_docs, total_success_docs, lock)
    _decrement_num_threads(lock)


def _increment_num_threads(lock):
    global num_threads
    with lock:
        num_threads += 1
        logger.debug("Thread - Increment # of active threads {}".format(num_threads))


def _decrement_num_threads(lock):
    global num_threads
    with lock:
        num_threads -= 1
        logger.debug("Thread - Decrement # of active threads {}".format(num_threads))


def _update_stats(total_failed_docs, total_success_docs, lock):
    global success_count, failure_count, processed_docs_count
    with lock:
        success_count += total_success_docs
        failure_count += total_failed_docs
        processed_docs_count += (total_success_docs + total_failed_docs)


def sanitize(source, destination):
    elastic_search_client = client.ElasticSearch((source.user, source.password), source.hostname,
                                                 (destination.user, destination.password), destination.hostname)
    elastic_search_client.clone_index(source_index_name=source.index,
                                      destination_index_name=destination.index)
    total_docs = elastic_search_client.get_total_docs_in_index(index_name='lcp_v2')
    results, next_scroll_id = elastic_search_client.get_docs(batch_size=100)

    # total_success_docs, total_failed_docs = _sanitize_and_insert(results, elastic_search_client, destination.index)
    # _update_stats(total_failed_docs, total_success_docs)

    lock = threading.RLock()
    global num_threads
    prev_scroll_id = ''
    while (num_threads < MAX_THREADS) and len(results) > 0:
        t = threading.Thread(target=_process, args=(results, elastic_search_client, destination.index, lock))
        logger.debug('Created new thread results {}'.format(len(results)))
        t.start()
        logger.debug('Fetching next set of results')
        results, next_scroll_id = elastic_search_client.get_scroll(scroll_id=next_scroll_id)
        logger.debug("# of active threads {}".format(num_threads))

    ## For each batch, save batch deets and status in a file
    ## For each batch, assign to a worker process to run jq
    ## For each batch, if a worker process fails to complete, re-run once more, log output
    _wait_for_threads_to_complete()
    global success_count, failure_count, processed_docs_count
    logger.debug('Final results: success_count {}, failure_count {}, processed_docs_count {}, num_threads {}'.format(
        success_count, failure_count, processed_docs_count, num_threads))
    pass


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
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    source = EsHost(hostname=args.source, index=args.source_index, user=args.user, password=args.password)
    destination = EsHost(hostname=args.destination, index=args.destination_index, user=args.destination_user,
                         password=args.destination_password)
    reset_logs()
    sanitize(source, destination)
