from autocore.lazylama.envs import env, env_prop
from elasticsearch import Elasticsearch

DEF_VAL = 500


class ES:
    host = env_prop(env.str, 'ES_URL', 'http://10.135.65.99:9200')
    default_limit = env_prop(env.int, 'ES_DEFAULT_SIZE_LIMIT', DEF_VAL)
    field_values_limit = env_prop(env.int, 'ES_FIELD_VALUES_LIMIT', DEF_VAL)
    mapping_field = env_prop(env.str, 'ES_INDEX_MAPPING_FIELD', '')

    def connection(self):
        return Elasticsearch(hosts=[self.host], retry_on_timeout=True)
