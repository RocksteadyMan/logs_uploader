from functools import cache
from typing import Any, Dict, List

from elasticsearch.exceptions import TransportError
from mergedeep import merge

from es_logs.constants import INDEX_SETTINGS, INDEX_REFRESH_INTERVAL
from es_logs.configs.elasticsearch import ES
from elasticsearch.helpers import scan

QUERY_ALL = '*'
STAR = '*'
INDEX_SETTINGS['settings']['index.refresh_interval'] = INDEX_REFRESH_INTERVAL


class ESHelper:
    def __init__(self):
        self.es_config = ES()
        self.es = self.es_config.connection()
        self.es.info = cache(self.es.info)
        self.defaultIndexSettings = INDEX_SETTINGS

    def query_factory(self):
        return QueryFactory(self.es_config)

    def list_indices(self):
        raw = self.es.indices.get(QUERY_ALL)
        indices = list(reversed(sorted(raw.keys())))
        result = []
        for index in indices:
            # skip system indices
            if index.startswith('.'):
                continue
            aliases = list(raw[index]['aliases'].keys())
            mappings = []
            if self.es_config.mapping_field:
                search = self.es.search(body={
                    'aggs': {
                        'unique': {
                            'terms': {'field': self.es_config.mapping_field + '.keyword', "size": 100}
                        }
                    },
                    'size': 0
                }, index=index)
                if search.get('aggregations'):
                    mappings = [bucket['key'] for bucket in search['aggregations']['unique']['buckets']]
            if not mappings:
                mappings.extend(raw[index]['mappings'].keys())
            result.append({
                'index': index,
                'alias': aliases if aliases else '',
                'mappings': mappings
            })
        return result

    def get_mapping_fields(self, indices):
        return {index: mapping['mappings'].get('properties', {}) for index, mapping in
                self.es.indices.get_mapping(indices).items()}

    def get_heatmap_metrics(self, indices, metrics=None, query=None):
        if metrics and not query:
            return sorted(metrics)

        search_results = \
            self.es.search(index=indices, body={"query": {'match': {'type': 'heatmap'}}}, size=10000)['hits']['hits']
        if not search_results:
            return []

        query = query if query else {}
        metrics_info = {}
        handled_query = {key: value for key, value in query.items() if value and value != QUERY_ALL}
        for item in search_results:
            if all(map(lambda x: item['_source'][x[0]] in x[1], handled_query.items())):  # True if query is empty
                for _type, _metrics in item['_source'].get('heatmap_metrics', {}).items():
                    for metric in _metrics:
                        metrics_info[metric['Id']] = f"{{{_type[:-6]}}}_{metric['Name']}"
        return [metrics_info[key] for key in sorted(metrics_info.keys())]

    def get_metrics_by_mapping(self, indices, mapping):  # very heavy method for big indexes
        metrics = []

        query = {"query": {"match": {"type": mapping}}}
        for hit in scan(self.es, index=indices, query=query, clear_scroll=True, size=5000):
            metrics.extend(hit['_source'].keys())
            metrics = list(set(metrics))
        return list(set(metrics))

    def is_index_exist(self, index: str) -> bool:
        return self.es.indices.exists(index)

    def update_index_mapping(self, index: str, mapping: Dict[str, dict]) -> None:
        mappings = {'properties': mapping}
        try:
            self.es.indices.put_mapping(index=index, body=mappings)
        except TransportError as e:
            raise Exception(f"Failed to update index mapping: {e.info}")

    def create_index(self, index: str, mapping: Dict[str, dict] = None, settings: Dict[str, dict] = None) -> None:
        settings = settings if settings else self.defaultIndexSettings
        mappings = {
            'mappings': {
                'properties': mapping
            }
        } if mapping else {}
        body = {**settings, **mappings}
        self.es.indices.create(index, body=body)

    def copy_docs(self, source: str, dest: str, query: dict, wait_for_completion: bool = False) -> str:
        body = {
            "source": {
                "index": source,
                "query": query
            },
            "dest": {
                "index": dest
            }
        }

        mappings = self.get_mapping_fields(source)
        for mapping in mappings.values():
            if not self.is_index_exist(dest):
                self.create_index(dest, mapping, settings=self.defaultIndexSettings)
                continue
            self.update_index_mapping(dest, mapping)

        task = self.es.reindex(body=body, wait_for_completion=wait_for_completion)
        return task.get('task')

    def delete_logs(self, source: str, body: dict, wait_for_completion: bool = False) -> str:
        task = self.es.delete_by_query(index=source, body=body, wait_for_completion=wait_for_completion)
        return task.get('task')

    def get_indices_settings(self, indices: List[str]) -> Dict[str, Any]:
        return self.es.indices.get_settings(index=indices)

    def get_task_info(self, task_id):
        info = self.es.tasks.get(task_id=task_id)
        return info['task']['status']

    def get_index_stats(self, indices):
        stats = self.es.indices.stats(index=indices)
        return stats['indices']

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])


class Aggs(object):
    DISTINCT = 'distinct'
    OVERALL = 'overall'
    PERCENTILES = 'pct'
    HISTOGRAM = 'histo'
    VALUES = 'values'
    ALL = 'all'


class QueryFactory(object):
    def __init__(self, es_config):
        self.es_config = es_config

    def __call__(self, indices, mappings=None, query=None, extra_query=None):
        return ESQuery(es_config=self.es_config, indices=indices, mappings=mappings, query=query)


class ESQuery(object):

    def __init__(self, es_config, indices, mappings, query=None):
        self.es_config = es_config
        self.es = self.es_config.connection()
        self.string_query = query
        self.indices = indices
        self.mappings = mappings
        self.result_callbacks = {}
        self.request = self._get_default_request()

    # deprecated
    # todo: remove after tests
    def _get_query_string(self):
        return ' AND '.join([f'({" OR ".join([f"{name}:{value}" for value in values])})'
                             for name, values in self.string_query.items() if values])

    @staticmethod
    def _get_all_query():
        request = {
            'must': {
                'query_string': {
                    'query': QUERY_ALL
                }
            }
        }
        return request

    def _get_should_query_string(self) -> dict:
        def make_term(n: str, val: Any) -> dict:
            return {'term': {n: val}}

        result = list()

        keys = set()

        for name, values in self.string_query.items():
            if values != STAR:
                if isinstance(values, str) or isinstance(values, int):
                    result.append(make_term(name, values))
                    continue
                if isinstance(values, list):
                    for v in values:
                        keys.add(name)
                        result.append(make_term(name, v))
                    continue

        if result:
            return {
                'should': result,
                'minimum_should_match': len(keys),
            }
        else:
            return self._get_all_query()

    def _get_default_request(self):
        request = {
            'query': {
                'bool': self._get_all_query() if not self.string_query else self._get_should_query_string()
            }
        }
        # Filter docs with chosen log type
        if self.es_config.mapping_field:
            request['query']['bool'].update({
                'filter': {
                    'term': {self.es_config.mapping_field: self.mappings}
                }
            })
        return request

    def _update_query(self, query):
        self.request = merge(self.request, query)

    def fetch(self):
        request_data = dict(body=self.request, index=self.indices, timeout='30s')
        response = self.es.search(**request_data)
        result = {}
        for name, callback in self.result_callbacks.items():
            result[name] = callback(response)
        return result

    def range(self, from_ts, to_ts):
        query = {
            'query': {
                'bool': {
                    'filter': [
                        {'range': {
                            'Time': {
                                'from': from_ts,
                                'to': to_ts
                            }
                        }}
                    ]
                }
            }
        }
        self._update_query(query)

    def distinct(self, field, size=None):
        aggs = {
            'size': 0,
            'aggs': {
                'distinct': {
                    'terms': {
                        'field': field,
                        'size': size if size else self.es_config.default_limit
                    }
                }
            }
        }

        self._update_query(aggs)
        self.result_callbacks.update({
            Aggs.DISTINCT: lambda res: sorted([el['key'] for el in res['aggregations'][Aggs.DISTINCT]['buckets']])
        })
        return self

    def overall(self, metric):
        aggs = {
            'size': 0,
            'aggs': {
                Aggs.OVERALL: {
                    'extended_stats': {
                        'field': metric
                    }
                }
            }
        }
        self._update_query(aggs)
        self.result_callbacks.update({
            Aggs.OVERALL: lambda res: res['aggregations'][Aggs.OVERALL]
        })
        return self

    def pct(self, metric, percents):
        aggs = {
            'size': 0,
            'aggs': {
                Aggs.PERCENTILES: {
                    'percentiles': {
                        'field': metric,
                        'percents': percents,
                        "hdr": {
                            "number_of_significant_value_digits": 4
                        }
                    }
                }
            }
        }
        self._update_query(aggs)
        self.result_callbacks.update({
            Aggs.PERCENTILES: lambda res:
            {f'{p.replace(".0", "")}th': v if v != 'NaN' else None
             for p, v in res['aggregations'][Aggs.PERCENTILES]['values'].items()}
        })
        return self

    def histo(self, metric, interval='1s', stats=('max', 'avg', 'min'), field='Time'):
        aggs = {
            'size': 0,
            'aggs': {
                Aggs.HISTOGRAM: {
                    'date_histogram': {
                        'field': field,
                        'fixed_interval': interval
                    },
                    'aggs': {
                        'bucket_stats': {
                            'stats': {
                                'field': metric
                            }
                        }
                    }
                }
            }
        }
        self._update_query(aggs)
        self.result_callbacks.update({
            Aggs.HISTOGRAM: lambda res: [
                [hit['key'], {stat: hit['bucket_stats'].get(stat) for stat in stats}]
                for hit in res['aggregations'][Aggs.HISTOGRAM]['buckets'] if hit['bucket_stats']['count'] != 0
            ]
        })
        return self

    def field_values(self, field, size=None):
        aggs = {
            'size': size if size else self.es_config.default_limit,
            "query": {
                'bool': {
                    'filter': {
                        "exists": {
                            "field": field
                        }
                    }
                }
            }
        }
        self._update_query({'query': {'bool': {'filter': None}}})
        self._update_query(aggs)
        self.result_callbacks.update({
            Aggs.VALUES: lambda res: res['hits']['hits']
        })
        return self

    def all(self):
        self.result_callbacks.update({
            Aggs.ALL: lambda res: res['hits']['hits']
        })
        return self
