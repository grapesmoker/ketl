from abc import abstractmethod
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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


class DataFrameLoader(BaseLoader):

    class FileFormat(Enum):

        PARQUET = 0
        CSV = 1

    def __init__(self, destination: Union[Path, str], **kwargs):
        super().__init__(destination)
        self.dest_path = Path(self.destination)

        self.writer = None
        self.file_format = None
        self.kwargs = kwargs

        self.dest_path.unlink(missing_ok=True)

        if self.dest_path.suffix.lower() == '.parquet':
            self.file_format = self.FileFormat.PARQUET
        elif self.dest_path.suffix.lower() in {'.csv', '.tsv'}:
            self.file_format = self.FileFormat.CSV
        else:
            raise ValueError(f'Unknown file type: {self.dest_path.suffix.lower()}')

    def _load_csv(self, data_frame: pd.DataFrame, **kwargs):

        with open(self.destination, 'a') as f:
            data_frame.to_csv(f, **kwargs)

    def _load_parquet(self, data_frame: pd.DataFrame, **kwargs):

        table = pa.Table.from_pandas(data_frame)
        if not self.writer:
            self.writer = pq.ParquetWriter(self.destination, table.schema)
        self.writer.write_table(table)

    LOADER_MAP = {
        FileFormat.PARQUET: _load_parquet,
        FileFormat.CSV: _load_csv
    }

    def load(self, data_frame: pd.DataFrame):

        loader = self.LOADER_MAP[self.file_format]
        # have to explicitly pass self here because we're not calling it via self._load_whatever
        loader(self, data_frame, **self.kwargs)

    def finalize(self):

        if self.writer:
            self.writer.close()


class DatabaseLoader(BaseLoader):

    def __init__(self, destination: str, **kwargs):

        self.engine = get_engine()
        self.schema = kwargs.pop('schema', None)
        self.kwargs = kwargs

        if self.schema:
            super().__init__(f'{self.schema}.{destination}')
            self.delete_stmt = text(f'DELETE FROM {self.schema}.{self.destination}')
        else:
            super().__init__(destination)
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
