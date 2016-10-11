import elasticsearch


class ElasticSearch(object):
    def __init__(self, (user, password), host=None):
        self._user = user
        self._password = password
        self._es = elasticsearch.Elasticsearch(
            hosts=['http://elastic-aws-ft.lxc.points.com:9200'])

    def get_total_docs_in_index(self, index_name):
        response = self._es.indices.stats(index=[index_name], metric='docs')
        import ipdb
        ipdb.set_trace()
