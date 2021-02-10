=====================
Questions and Answers
=====================

What is this for?
-----------------

It's for getting lots of data from one place to another, in a configurable way. Over time I found
myself continuously writing functions that fetched data from somewhere, munged it, and then put
it somewhere else. This is of course the classic ETL problem, and kETL is an attempt to make this
systematic without getting into extremely complicated setups.

Why do I need to subclass everything?
-------------------------------------

kETL enforces a strict separation between the ETL phases, and it is engineered so that the output
of one phase directly enters the input of the other. Thus, extractors are linked to transformers,
which are themselves linked to loaders. Transformers and loaders in turn need to know something
about the data that they are operating on. In the worked example included with the project,
you can see that data available from the Census website has slightly different schemas depending
on whether it's three-way, four-way, etc. Accommodating the different data structures is up
to the user; it is possible that it may be accommodated with a single transformer, or you
may need to create separate transformer classes for different sorts of data.

Subclassing the different components and overriding default behaviors as needed can result in
some inefficiencies of processing (for example, files might need to be traversed multiple times),
but the benefit is simplicity of configuration and readability of code. Unless you're dealing
with scales at which this project would be inappropriate anyway, it seems like a small price to pay.

How should I organize my APIs, sources, and cached files?
---------------------------------------------------------

It's really up to you. I tend to designate sources as being the most specific URL that you
can specify without getting to the actual file. All the files dwelling under that hierarchy
become cached files. But it very much depends on the structure of the data you're trying
to fetch.

For APIs, I tend to favor organizing them by general data schema. So if I'm getting data from
the Census Bureau and all the Business Dynamics data can fit under a common framework, that's
an API. If I need to get some other dataset from the Census that has a very different schema,
I would subclass API again and set it up separately.

How should I organize my extractors and transformers?
-----------------------------------------------------

The answer to this question largely depends on the structure of the data you are trying to
process. If all of your data is tabular and all you want to do is to turn it into something
that can be ingested by a database, you can have multiple extractors feed one transformer
that will do the job. If, on the other hand, the data is more complex, such as a JSON file
that may contain a number of different datasets, you could do the reverse and have one
extractor feed multiple transformers. It all depends on the data structure and kETL is
entirely agnostic to this.
