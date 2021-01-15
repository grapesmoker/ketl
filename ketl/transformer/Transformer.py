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


class BaseTransformer:

    # TODO: init should take the configuration kwargs

    SCHEMA = None
    COLUMN_NAMES = None

    def __init__(self, **kwargs):

        self.passed_kwargs = kwargs

    @abstractmethod
    def transform(self, source_files: List[Path]) -> pd.DataFrame:

        raise NotImplementedError

    @abstractmethod
    def _build_data_frame(self, source_files: List[Path]) -> pd.DataFrame:

        raise NotImplementedError


class DelimitedTableTransformer(BaseTransformer):

    def __init__(self, **kwargs):

        super(DelimitedTableTransformer, self).__init__(**kwargs)
        self.transpose = self.passed_kwargs.pop('transpose', False)
        self.concat_on_axis = self.passed_kwargs.pop('concat_on_axis', None)

        self.reader_kwargs = {
            'comment': None,
            'names': None,
            'delimiter': None,
            'header': 'infer',
            'dtype': None,
            'index_col': None,
            'parse_dates': None,
            'skiprows': None,
            'iterator': True,
            'chunksize': 50000
        }
        self.reader_kwargs.update(self.passed_kwargs)

    def _build_data_frame(self, source_files: List[Path]):

        data_frames = [pd.read_csv(source_file, **self.reader_kwargs) for source_file in source_files]

        # for the special case where every file is a column. this assumes all data can fit into memory
        if self.concat_on_axis:
            df = pd.concat(data_frames, axis=self.concat_on_axis)
            yield df
        else:
            df_chain = chain(*data_frames)

            for chunk in df_chain:
                if self.transpose:
                    yield chunk.transpose()
                else:
                    yield chunk

    def transform(self, source_files: List[Path]) -> pd.DataFrame:

        for df in self._build_data_frame(source_files):
            yield df


class MatrixWriterMixin:

    @abstractmethod
    def to_matrix(self, output_dir: Path) -> Path:
        raise NotImplementedError

