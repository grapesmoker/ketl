from abc import abstractmethod
from itertools import chain
from pathlib import Path
from typing import List, Union, Dict, Callable

import pandas as pd
import json


class AdapterError(Exception):
    pass


class NoValidSourcesError(AdapterError):
    pass


class BaseTransformer:
    """
    The base transformer class. Should not be instantiated directly.
    """
    # TODO: init should take the configuration kwargs

    def __init__(self, transpose: bool = False, concat_on_axis: Union[int, str] = None,
                 columns: List[Union[str, int]] = None, skip_errors: bool = False,
                 rename: Union[Callable, Dict[str, str]] = None, **kwargs):
        """ Initialize the transformer.

        :param transpose: whether to transpose the resulting matrix.
        :param concat_on_axis: whether to concatenate data along some axis.
        :param columns: column names.
        :param skip_errors: whether to skip input files if an error is encountered.
        :param rename: a dict or function suitable for passing to the Pandas rename function.
        :param kwargs: optional keyword arguments to pass to reader.
        """
        self.transpose = transpose
        self.concat_on_axis = concat_on_axis
        self.columns = columns
        self.skip_errors = skip_errors
        self.rename = rename

        self.passed_kwargs = kwargs

    @abstractmethod
    def transform(self, source_files: List[Path]) -> pd.DataFrame:
        """ Run the actual transformation.

        :param source_files: the source files containing the data.
        :return: a data frame.
        """
        raise NotImplementedError

    @abstractmethod
    def _build_data_frame(self, source_files: List[Path]) -> pd.DataFrame:
        """ Construct a data frame from the list of inpute files.

        :param source_files: the source files containing the data.
        :return: a data frame.
        """
        raise NotImplementedError


class DelimitedTableTransformer(BaseTransformer):
    """
    A transformer that changes the input data into a delimited table.
    """
    def __init__(self, transpose: bool = False, concat_on_axis: Union[str, int] = None,
                 columns: List[Union[str, int]] = None, skip_errors: bool = False,
                 rename: Union[Callable, Dict[str, str]] = None, **kwargs):
        """ Initialize the transformer.

        :param transpose: whether to transpose the resulting data.
        :param concat_on_axis: whether to concatenate the data along an axis.
        :param columns: list of column names.
        :param skip_errors: whether to skip errors.
        :param rename: a dict or function suitable for passing to the Pandas rename function.
        :param kwargs: keyword arguments to be passed to the reader.
        """
        super(DelimitedTableTransformer, self).__init__(
            transpose, concat_on_axis, columns, skip_errors, rename, **kwargs)

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
        """ Build a data frame from a list of source files. All kwargs set at initialization are passed
        to the CSV reader.

        :param source_files: a list of source files to read data from.
        :return: a Pandas data frame.
        """
        data_frames = [pd.read_csv(source_file, **self.reader_kwargs) for source_file in source_files]

        # for the special case where every file is a column. this assumes all data can fit into memory
        # TODO: replace this with dask stuff so that things can be lazily concatenated
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
        """ Transform the data contained in the list of source files to something else. By default
        simply returns the data frame consisting of the raw data.

        :param source_files: a list of source files.
        :return: a Pandas data frame.
        """
        for df in self._build_data_frame(source_files):
            yield df


class JsonTableTransformer(BaseTransformer):

    def __init__(self, record_path: Union[List[str], str] = None,
                 transpose: bool = False,
                 concat_on_axis: Union[str, int] = None,
                 columns: List[Union[str, int]] = None,
                 skip_errors: bool = False,
                 rename: Union[Callable, Dict[str, str]] = None,
                 **kwargs):

        super(JsonTableTransformer, self).__init__(
            transpose, concat_on_axis, columns, skip_errors, rename, **kwargs)

        self.record_path = record_path

        self.reader_kwargs = {
            'orient': None,
            'typ': 'frame',
            'dtype': None,
            'convert_axes': None,
            'convert_dates': True,
            'keep_default_dates': True,
            'precise_float': False,
            'date_unit': None,
            'encoding': None,
            'lines': False,
            'chunksize': None,
            'compression': 'infer',
            'nrows': None,
            'storage_options': None
        }
        self.reader_kwargs.update(self.passed_kwargs)

    @staticmethod
    def _extract_data(filename: Union[Path, str], record_path: Union[List[str], str],
                      serialize: bool = True) -> Union[dict, list, str]:

        with open(filename, 'r') as f:
            data: dict = json.load(f)
            if type(record_path) is str:
                if serialize:
                    return json.dumps(data[record_path])
                else:
                    return data[record_path]
            elif type(record_path) is list:
                for item in record_path:
                    data = data[item]
                if serialize:
                    return json.dumps(data)
                else:
                    return data
            else:
                raise TypeError('record_path must be a list or a string')

    def _build_data_frame(self, source_files: List[Path]) -> pd.DataFrame:

        # we're assuming any single json file can fit into memory here because we need to be able to
        # access its internals to extract data from it

        for source_file in source_files:

            try:
                if not self.record_path:
                    df = pd.read_json(source_file, **self.reader_kwargs)
                    df._source_file = source_file
                else:
                    data = self._extract_data(source_file, self.record_path)
                    df = pd.read_json(data, **self.reader_kwargs)
                    df._source_file = source_file

                yield df.transpose() if self.transpose else df
            except Exception as ex:
                if self.skip_errors:
                    print(f'skipping {source_file} due to error: {ex}')
                    yield pd.DataFrame()
                else:
                    raise ex

    def transform(self, source_files: List[Path]) -> pd.DataFrame:

        # TODO: move the renaming logic to the base and allow a mapper to be passed

        for df in self._build_data_frame(source_files):
            if not df.empty:
                if self.rename:
                    df = df.rename(self.rename, axis='columns')
                if self.columns:
                    df = df[self.columns]

            yield df
