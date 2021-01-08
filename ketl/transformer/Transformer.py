from abc import abstractmethod
from itertools import chain
from pathlib import Path
from typing import List, Tuple, Union, Optional, Dict
from tqdm import tqdm

import pandas as pd
import numpy as np

from ketl.db.settings import get_session


class AdapterError(Exception):
    pass


class NoValidSourcesError(AdapterError):
    pass


class Transformer:

    SCHEMA = None
    COLUMN_NAMES = None
    DB_TABLE = None
    DB_SCHEMA = None

    def __init__(self, output_directory: Union[str, Path], source_files: List[str]):

        self.output_directory = output_directory
        self.source_files = source_files

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        raise NotImplementedError

    @abstractmethod
    def build_data_frame(self):

        raise NotImplementedError

    @abstractmethod
    def process(self, dat_output_dir):

        raise NotImplementedError


class DelimitedTableTransformer(Transformer):

    COLUMN_NAMES = None
    SCHEMA = None
    COMMENT = None
    DELIMITER = None
    DATE_COLS = None
    INDEX_COL = None
    HEADER = 'infer'
    TRANSPOSE = False
    SKIP_ROWS = None
    CONCAT_ON_AXIS = None
    ITERATOR = True

    CHUNK_SIZE = 50000

    def build_data_frame(self):

        data_frames = [pd.read_csv(source_file, comment=self.COMMENT, names=self.COLUMN_NAMES, header=self.HEADER,
                                   delimiter=self.DELIMITER, dtype=self.SCHEMA, index_col=self.INDEX_COL,
                                   skiprows=self.SKIP_ROWS, parse_dates=self.DATE_COLS, iterator=self.ITERATOR,
                                   chunksize=self.CHUNK_SIZE)
                       for source_file in self.source_files]

        # for the special case where every file is a column. this assumes all data can fit into memory
        if self.CONCAT_ON_AXIS:
            df = pd.concat(data_frames, axis=self.CONCAT_ON_AXIS)
            yield df
        else:
            df_chain = chain(*data_frames)

            for chunk in df_chain:
                if self.TRANSPOSE:
                    yield chunk.transpose()
                else:
                    yield chunk

    def transform(self, df: pd.DataFrame):

        return df

    def process(self, dat_output_dir: Path = None):

        pass


class MatrixWriterMixin:

    @abstractmethod
    def to_matrix(self, output_dir: Path) -> Path:
        raise NotImplementedError

