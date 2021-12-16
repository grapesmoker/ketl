=====
Usage
=====

kETL is a library of components intended to execute the different stages of the ETL pipeline.
These components are the extractors, transformers, and loaders. Additionally, there is a Pipeline
class which provides a convenience logic for chaining complicated ETL configurations together.

Philosophy
----------

The idea behind kETL is to turn the ETL process into a configurable set of building blocks
that can be flexibly reused as needed. Rather than writing custom functions for every sort of
extraction and transformation logic under the sun, kETL strictly enforces a separation of concerns,
demanding that you subclass and override different functions provided to achieve the desired
goal.

In particular, kETL's data model emphasizes not repeating data fetches if the existing data
has not changed. To do that, kETL keeps track of the hashes of the files it has downloaded
and will only redownload them if forced to or if the statistics of the file (e.g. size) have
changed. kETL can also redownload a file if a certain configurable amount of time has passed,
under the assumption that the contents of that file have changed.

Example
-------

Suppose I want to get a bunch of data from the Census Bureau. Here's a worked example of how
I might do that. First, we need to configure the API.

.. code-block:: python

    from ketl.db import models
    from ketl.extractor import DefaultExtractor
    from ketl.transformer import DelimitedTableTransformer
    from ketl.loader import DelimitedFileLoader, DatabaseLoader
    from ketl.utils.db_utils import get_or_create

    class CensusAPI(models.API):

        def setup():

            source, _ = get_or_create(Source,
                url='http://www2.census.gov/programs-surveys/bds/tables/time-series/',
                data_dir='downloads',
                api_config_id=self.id)

            cached_file, _ = get_or_create(CachedFile,
                url='bds2018.csv',
                expected_mode=models.ExpectedMode.self)

This is the entirety of the setup to grab the economy-wide dataset from the business dynamics
page of the Census Bureau. A few things merit mention:

* :code:`get_or_create` is a utility function which returns a database instance with the
  supplied parameters, or creates one if such an instance does not exist. It also returns
  whether or not the instance was created, which can be ignored unless you need to know this.
* If the :class:`ExpectedFile` is equivalent to the CachedFile which is to be downloaded, the
  :code:`expected_mode` argument of the :class:`CachedFile` constructor can be set to
  :class:`ExpectedMode.success`. This will automatically create an :class:`ExpectedFile` with
  identical information to the downloaded file.
* Although we have here defined the configuration statically, we need not do so as long as we
  have some place to begin. For example, it would be just as correct to scrape the public-facing
  website of the Census Bureau and generate the :class:`CachedFile` entries dynamically that way.
  In fact, that would be a paradigmatic usage of kETL.

Once the API is configured, we are off to the races:

.. code-block:: python

    api = models.API.get_instance(CensusAPI)
    api.setup()
    extractor = DefaultExtractor(api)
    files = extractor.extract()

The extractor will now fetch all the entries defined by the :code:`setup` function above and
put them in the :code:`download` directory relative to the current working directory. Easy!
Note the use of the static :code:`get_instance` convenience function, which will return an
instance of the CensusAPI, or create one if one does not exist in the database. API instances
must have unique names; see the data model section below for details.

We can now transform the CSV data into something else. For the time being we will simply turn
it into a Parquet file with all the same data.

.. code-block:: python

    transformer = DelimitedTableTransformer()
    loader = DatabaseLoader('bds_economy')
    for df in transformer.transform(files):
        loader.load(df)

This will incrementally process the data in the downloaded files and load it into whatever
database you have configured. Now the data is yours to do with as you please.

The transformers and loaders all include additional options that can be read in the docstrings
of the specific classes. Note that the :class:`DatabaseLoader` is agnostic to the underlying data
model of the passed data frame; it leaves it to Pandas to convert the data into the proper
SQL.


Data model
----------

At the heart of kETL is a data model consisting of APIs, Sources, CachedFiles, and ExpectedFiles.
Their functions are described below.

API
+++

The :class:`~ketl.db.models.API` class is the basic unit of configuration around which everything
revolves. The API has one :code:`setup` method that must be executed before it can be used; this
method should be used to configure the rest of the data that is to be fetched. :code:`API` must
be subclassed and the :code:`setup` method overridden by the user. The API may optionally be
given a name, though if one is not given, the API will use the name of the class itself. Note
that *only one API of a given name* may exist in a project.

Source
++++++

The :class:`~ketl.db.models.Source` class represents some actual location of data nested under
an :class:`~ketl.db.models.API`. The purpose of the :class:`~ketl.db.models.Source` is twofold:
to configure the base location of the data and to configure where on disk the data should be
placed. The :class:`~ketl.db.models.Source` itself does not actually configure any files to be
extracted, it merely provides a structure for their organization.

CachedFile
++++++++++

The :class:`~ketl.db.models.CachedFile` class represents an actual file to be downloaded from
somewhere. The location of the file may be either an FTP server or any location that is
accessible by :code:`smart_open`. Note that the URL parameter of :class:`~ketl.db.models.CachedFile`
should be specified *relative to the URL of its source*. In other words, if the
:class:`~ketl.db.models.Source` has the :code:`base_url` of :code:`https://path/to/some`
then to retrieve the a file under this hierarchy we would create a :class:`~ketl.db.models.CachedFile`
whose url is :code:`file`; this will be joined with the URL of the source to produce the
actual resource to be retrieved. Similarly, the :code:`path` of :class:`~ketl.db.models.CachedFile`
should be relative to the :code:`data_dir` of the parent :class:`~ketl.db.models.Source`.

ExpectedFile
++++++++++++

The :class:`~ketl.db.models.ExpectedFile` class reflects files that actually appear on disk.
For example, the :class:`~ketl.db.models.CachedFile` may represent an archive that might need to
be decompressed somewhere; the :class:`~ketl.db.models.ExpectedFile` might represent a file that
is actually present within the archive. It is the :class:`~ketl.db.models.ExpectedFile` s that
represent the data that is actuall to be processed by :class:`~ketl.transformer.Transformer` s.

Functional Components
---------------------

Extractors
++++++++++

The :class:`Extractor` class is the direct link between the data model and the actual ETL operations.
The job of the :class:`Extractor` is to actually fetch the :class:`CachedFile` entries from wherever
they happen to reside. The initializer of the :class:`Extractor` takes an :class:`API` instance
as an argument. Assuming :code:`setup` has been called on the :class:`API`, the :class:`Extractor`
can then be run with the :code:`extract` function and will download the specified data.

Transformers
++++++++++++

The job of the :class:`Transformer` is to take the :class:`ExpectedFiles` generated by the
:class:`Extractor` and transform them in some way. There are two default transformers that are
part of kETL: the :class:`DelimitedTableTransformer` and the :class:`JsonTableTransformer`.
All :class:`Transformer` subclasses of the :class:`BaseTransformer` parent class must implement
the :code:`transform` method, but they may also override any of the other methods as needed.
The transformers must produce a Pandas data frame, which is then passed to the loader.

Loaders
+++++++

The :class:`Loader` class is responsible for the final stage of the pipeline: putting the data
somewhere, either on disk or to a database. The :class:`DelimitedFileLoader` writes a data frame
to disk a CSV file, and the :class:`DatabaseLoader` loads the data into
a database table. Any database that can be interfaced with via SQLalchemy should work fine.

