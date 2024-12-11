import redis
import duckdb
import pandas as pd


class Querier():
    def __init__(self):
        self.duckdb = duckdb.connect('../duckdb/duck.db')
        self.redis = redis.Redis(host='redis', port=6379, db=0)


    def query_cached(self, *, sql: str, cache_key: str) -> pd.DataFrame:
        res = self.redis.get(cache_key)

        if res is None:
            res = self.duckdb.sql(sql).fetchall()

        return pd.DataFrame(res)


