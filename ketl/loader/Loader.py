from abc import abstractmethod
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Union, Any, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pickle

from sqlalchemy import text

from ketl.db.settings import get_engine


class InvalidLoaderConfiguration(Exception):
    pass


class BaseLoader:

    def __init__(self, destination: Union[Path, str], **kwargs):

        self.destination = destination

    @abstractmethod
    def load(self, data_frame: pd.DataFrame):

        raise NotImplementedError

    @abstractmethod
    def finalize(self):

        raise NotImplementedError


class HashLoader(BaseLoader):

    def load(self, data_frame: pd.DataFrame, **kwargs):

        df_hash = sha256(pd.util.hash_pandas_object(data_frame).values).hexdigest()
        with open(self.destination, 'w') as f:
            f.write(df_hash + '\n')

    def finalize(self):
        pass


class LocalFileLoader(BaseLoader):

    def __init__(self, destination: Union[Path, str], naming_func: Callable = None, **kwargs):

        super().__init__(Path(destination))
        self.naming_func = naming_func
        self.writer = None
        self.kwargs = kwargs

        # deletes compatible with pre-3.8 python

        if self.destination.is_dir():
            files = self.destination.glob('*')
            for file in files:
                if file.exists():
                    file.unlink()
        else:
            if self.destination.exists():
                self.destination.unlink()

    def full_path(self, df: pd.DataFrame):
        if not self.naming_func:
            return self.destination
        else:
            return self.destination / self.naming_func(df)

    def finalize(self):

        pass


class ParquetLoader(LocalFileLoader):

    def load(self, data_frame: pd.DataFrame):

        try:
            table = pa.Table.from_pandas(data_frame)
            if not self.writer:
                self.writer = pq.ParquetWriter(self.full_path(data_frame), table.schema)
            else:
                if not self.full_path(data_frame).exists():
                    self.writer.close()
                    self.writer = pq.ParquetWriter(self.full_path(data_frame), table.schema)
            self.writer.write_table(table)
        except Exception as ex:
            print(f'Could not process {self.full_path(data_frame)}')
            raise ex

    def finalize(self):

        if self.writer:
            self.writer.close()


class DelimitedFileLoader(LocalFileLoader):

    def load(self, data_frame: pd.DataFrame):
        with open(self.full_path(data_frame), 'a') as f:
            data_frame.to_csv(f, **self.kwargs)


class DatabaseLoader(BaseLoader):

    def __init__(self, destination: str, **kwargs):

        self.engine = get_engine()
        self.schema = kwargs.pop('schema', None)
        self.kwargs = kwargs
        super().__init__(destination)

        if self.schema:
            self.delete_stmt = text(f'DELETE FROM {self.schema}.{self.destination}')
        else:
            self.delete_stmt = text(f'DELETE FROM {self.destination}')

        self.clean = False

    def load(self, data_frame: pd.DataFrame):

        # this is somewhat less general than either having a proper pre/post load hook
        # logic or doing something like writing to a temp staging table and then moving
        # the final result to the desired destination, but it's also *a lot* simpler

        if not self.clean:
            self.engine.execute(self.delete_stmt)
            self.clean = True

        data_frame.to_sql(self.destination, self.engine, index=False, if_exists='append', schema=self.schema)

    def finalize(self):

        pass


class PickleLoader(BaseLoader):

    def __init__(self, pickler=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.pickler = pickler or pickle.dump

    def load(self, obj: Any):

        with open(self.destination, 'wb') as f:
            self.pickler(obj, f)

    def finalize(self):

        pass
