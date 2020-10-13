import math

from elasticsearch import Elasticsearch,helpers

class BaseEs(object):
    def __init__(self, host, port):
        self.es = Elasticsearch("{0}:{1}".format(host, port))

    def search(self, index, query_body):
        data = self.es.search(index=index, body=query_body)
        result = [doc["_source"] for doc in data['hits']['hits']]
        return result

    def count(self, index, query_body):
        data = self.es.count(index=index, body=query_body)
        return data['count']

    def bulk_data(self, index, method, ori_data, single_num=200):
        actions = []
        for data in ori_data:
            if data:
                action = self._format_bulk_func(index, method, data)
                actions.append(action)

                if len(actions) == single_num:
                    helpers.bulk(client=self.es, actions=actions)
                    actions = []

        if len(actions) > 0:
            helpers.bulk(client=self.es, actions=actions)

    def scroll_search(self, index, all_num, query_body, single_num=1000):
        datas = []
        scroll_id = None

        for i in range(math.ceil(all_num / single_num)):
            if not scroll_id:
                query_body['size'] = single_num
                response = self.es.search(index=index, scroll='2m', body=query_body)
            else:
                response = self.es.scroll(scroll_id=scroll_id, scroll='2m')

            if response:
                scroll_id = response['_scroll_id']
                response_data = [doc["_source"] for doc in response['hits']['hits']]
                datas.extend(response_data)

        return datas

    @staticmethod
    def _format_bulk_func(index, method, data):
        action = {
            "_index": index,
            "_op_type": method,
            "_type": "_doc",
        }

        if method == 'update':
            action['_id'] = data.pop('id')
            action['doc'] = data
        else:
            action['_source'] = data

        return action


if __name__ == "__main__":
    es = BaseEs('127.0.0.1', 9200)
    body = {
        "query":{
            "match_all": {}
        }
    }
    es.search('blog', body)





