import pandas as pd

from redis import Redis
from redis.commands.json.path import Path


class DataPipeline:
    def __init__(self, columns: dict, key: str, table_name: str, weights: dict) -> None:
        self.__columns = columns
        self.table_name = table_name
        self.__weights = weights

        self.dataframe = pd.read_feather("./data/dataframe.feather")
        self.index_key_col = key

        self.__redis_client: Redis = Redis(host="localhost", port=6379, db=0)

    def df_to_redisjson(self):
        data = self.dataframe.to_dict(orient="records")

        for item in data:
            self.__redis_client.json().set(
                f"{self.table_name}:{item[self.index_key_col]}", Path.root_path(), item
            )


if __name__ == "__main__":
    data_pipeline = DataPipeline(
        columns={
            "album_id": "string",
            "track_id": "string",
            "track_name": "string",
            "artist_name": "string",
            "cdn_id": "string",
            "cluster_id": "integer",
            "content_id": "string",
            "search": "string",
            "artist_id": "string",
            "album_popularity": "integer",
            "artist_popularity": "integer",
            "artist_genres": "string",
        },
        table_name="tracks",
        key="track_id",
        weights={},
    )

    data_pipeline.df_to_redisjson()
