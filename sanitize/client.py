import logging
import elasticsearch

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ElasticSearch(object):
    def __init__(self, (user, password), host=None):
        self._user = user
        self._password = password
        self._es = elasticsearch.Elasticsearch(
            hosts=['http://elastic-aws-ft.lxc.points.com:9200'])

    def get_total_docs_in_index(self, index_name):
        """
        Return the total number of docs in the index based on the 'primaries'.
        This is to not include replicated docs in the count.

        :param index_name:
        :return:
        """
        response = self._es.indices.stats(index=[index_name], metric='docs')
        logger.debug(response)
        try:
            return response['indices'][index_name]['primaries']['docs']['count']
        except KeyError:
            logger.error('Unable to get total docs in index.')
            raise
