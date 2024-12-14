import redis
import duckdb
import pickle
import pandas as pd


class CachedQuery():
    def __init__(self, *, duckdb_readonly=True):
        self.duckdb = duckdb.connect('../duckdb/duck.db', read_only=duckdb_readonly)
        self.redis = redis.Redis(host='redis', port=6379, db=0)


    def query_cached(self, *, sql: str, cache_key: str) -> pd.DataFrame:
        cache_res = self.redis.get(cache_key)

        if cache_res is None or True:
            res = self.duckdb.sql(sql).fetchall()
            df = pd.DataFrame(res)

            self.redis.set(cache_key, pickle.dumps(df))
        else:
            df = pickle.loads(cache_res)

        return df


