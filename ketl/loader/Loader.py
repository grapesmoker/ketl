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
    """ The base loader class. Not intended to be instantiated directly.
    """
    def __init__(self, destination: Union[Path, str], **kwargs):
        """ Initialize the loader.

        :param destination: the destination to which to load.
        :param kwargs: ignored.
        """
        self.destination = destination

    @abstractmethod
    def load(self, data_frame: pd.DataFrame):
        """ Abstract method that must be implemented by any subclass.
        Performs the actual load.
        """
        raise NotImplementedError

    @abstractmethod
    def finalize(self):
        """ Abstract method that must be implemented by any subclass.
        Finalizes any closing that needs to happen.
        """
        raise NotImplementedError


class HashLoader(BaseLoader):

    def load(self, data_frame: pd.DataFrame, **kwargs):
        """ Writes a hash to a file that contains the value of a data frame.

        :param data_frame: a Pandas data frame.
        :param kwargs: ignored.
        :return: None
        """
        df_hash = sha256(pd.util.hash_pandas_object(data_frame).values).hexdigest()
        with open(self.destination, 'w') as f:
            f.write(df_hash + '\n')

    def finalize(self):
        pass


class LocalFileLoader(BaseLoader):

    def __init__(self, destination: Union[Path, str], naming_func: Callable = None, clean: bool = True, **kwargs):

        """A loader the loads the data to a local file. Not intended to be instantiated directly.

        :param destination: the path of the destination.
        :param naming_func: a callable that derives the name of a file from the data frame.
        :param kwargs: optional key word arguments to the loader.
        """
        super().__init__(Path(destination))
        self.naming_func = naming_func
        self.writer = None
        self.kwargs = kwargs

        # deletes compatible with pre-3.8 python

        if clean:
            if self.destination.exists() and self.destination.is_dir():
                files = self.destination.glob('*')
                for file in files:
                    if file.exists():
                        file.unlink()
            elif self.destination.exists():
                self.destination.unlink()


    def full_path(self, df: pd.DataFrame):
        if not self.naming_func:
            return self.destination
        else:
            return self.destination / self.naming_func(df)

    def finalize(self):

        pass


class ParquetLoader(LocalFileLoader):
    """ A loader that writes data to a Parquet file.
    """
    def load(self, data_frame: pd.DataFrame):
        """ Write data to a Parquet file.

        :param data_frame: a Pandas data frame.
        :return: None
        """
        try:
            table = pa.Table.from_pandas(data_frame)
            if not self.writer:
                self.writer = pq.ParquetWriter(self.full_path(data_frame), table.schema)
            else:
                if not self.full_path(data_frame).exists():
                    self.writer.close()
                    self.writer = pq.ParquetWriter(self.full_path(data_frame), table.schema)
            self.writer.write_table(table)
        except Exception as ex:  # pragma: no cover
            print(f'Could not process {self.full_path(data_frame)}')  # pragma: no cover
            raise ex  # pragma: no cover

    def finalize(self):
        """ If a writer is open, close it.

        :return: None
        """
        if self.writer:
            self.writer.close()


class DelimitedFileLoader(LocalFileLoader):
    """ A loader that writes delimited data to a text file.
    """

    def load(self, data_frame: pd.DataFrame):
        """ Write the data to a delimited text file.
        :param data_frame: a Pandas data frame.
        :return: None.
        """
        with open(self.full_path(data_frame), 'a') as f:
            data_frame.to_csv(f, **self.kwargs)


class DatabaseLoader(BaseLoader):
    """ A loader that writes data to a database table. The table is presumed to already exist.
    """
    def __init__(self, destination: str, **kwargs):
        """ Initialize the loader and set up a delete statement to be issued to drop the data
        in the table.

        :param destination: the database table to write to.
        :param kwargs: optional keyword arguments. Schema is used to set up delete statement.
        """
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
        """ Write the data to a table. Deletes data if it hasn't been deleted already.

        :param data_frame:
        :return:
        """

        # this is somewhat less general than either having a proper pre/post load hook
        # logic or doing something like writing to a temp staging table and then moving
        # the final result to the desired destination, but it's also *a lot* simpler

        if not self.clean:
            self.engine.execute(self.delete_stmt)
            self.clean = True

        data_frame.to_sql(self.destination, self.engine, index=False, if_exists='append', schema=self.schema)

    def finalize(self):

        pass  # pragma: no cover


class PickleLoader(LocalFileLoader):
    """ A loader to write the data to a pickle file.
    """

    def __init__(self,destination: Union[Path, str], naming_func: Callable = None,
                 pickler=None, **kwargs):
        """ Initialize the loader. Sets up an alternative pickler if one is supplied (e.g. cloudpickle)

        :param destination: the file to which to write the data.
        :param naming_func: a function that derives the name of the file from the data frame.
        :param pickler: an optional pickler (e.g. cloudpickle.dump) to use instead of the standard one.
        :param kwargs: ignored.
        """
        super().__init__(destination, naming_func, **kwargs)
        self.pickler = pickler or pickle.dump

    def load(self, obj: Any):
        """ Write the data to the file using the pickler.

        :param obj:
        :return:
        """
        with open(self.destination, 'wb') as f:
            self.pickler(obj, f)

    def finalize(self):

        pass  # pragma: no cover
