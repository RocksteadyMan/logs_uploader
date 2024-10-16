from autocore.integration.mongo_lib import Mongo
from autocore.lazylama.envs import env, env_prop


class MongoConfig:
    connection = env_prop(env.str, 'MONGO_CONNECTION', '10.135.65.99:27017')
    db = env_prop(env.str, 'MONGO_DB', 'autoqa')

    def collection(self, collection_name):
        return Mongo(mongo_connection=self.connection, db=self.db)[collection_name]
