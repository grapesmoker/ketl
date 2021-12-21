import pandas as pd

from typing import Dict, List, Callable, Optional, Union, Type
from itertools import product
from pathlib import Path
from collections import defaultdict

from ketl.extractor.Extractor import BaseExtractor
from ketl.transformer.Transformer import BaseTransformer
from ketl.loader.Loader import BaseLoader


class InvalidPipelineError(Exception):
    pass


class ETLPipeline:

    def __init__(self, extractors: List[BaseExtractor] = None,
                 transformers: List[BaseTransformer] = None,
                 loaders: List[BaseLoader] = None,
                 fanout: Dict[Union[BaseExtractor, BaseTransformer, BaseLoader],
                              Union[List[BaseTransformer], List[BaseLoader]]] = None):
        """
        Initialize the ETL pipeline. If a fanout is provided, checks the fanout for validity and
        ignores any other parameters. If a fanout is not provided, construct one from the provided
        extractors, transformers, and loaders where each extractor feeds each transformer and
        each transformer feeds each loader.

        :param extractors: A list of Extractors.
        :param transformers: A list of Transformers.
        :param loaders: A list of Loaders.
        :param fanout: A fanout. Other parameters ignored if fanout is provided.
        """
        extractors = extractors or []
        transformers = transformers or []
        loaders = loaders or []

        if not fanout:
            self.fanout: Dict[Union[BaseExtractor, BaseTransformer, BaseLoader],
                              Union[List[BaseTransformer], List[BaseLoader]]] = defaultdict(list)
            for extractor, transformer in product(extractors, transformers):
                self.fanout[extractor].append(transformer)
            for transformer, loader in product(transformers, loaders):
                self.fanout[transformer].append(loader)
        else:
            for k, v in fanout.items():
                if isinstance(k, BaseExtractor) and not all(map(lambda item: isinstance(item, BaseTransformer), v)):
                    raise InvalidPipelineError(f'An Extractor {k} is attached to something other than a Transformer: {v}.')
                elif isinstance(k, BaseTransformer) and not all(map(lambda item: isinstance(item, BaseLoader), v)):
                    raise InvalidPipelineError(f'A Transformer {k} is attached to something other than a Loader: {v}.')
            self.fanout = fanout

    def execute(self):
        """
        Run the pipeline. Fires the extractors, then feeds the results into the transformers.
        :return: None
        """

        extraction_results = self._fire_extractors()
        self._fire_transformers(extraction_results)

    def _fire_extractors(self) -> Dict[BaseExtractor, List[Path]]:
        """
        Fire the extractors.
        :return: A dictionary whose keys are extractors and whose values are lists of paths
        produced by the extractors.
        """
        results = {op: op.extract() for op in self.fanout if isinstance(op, BaseExtractor)}

        return results

    def _fire_transformers(self, extraction_results: Dict[BaseExtractor, List[Path]]):
        """
        Fire the transformers. Each transformer result feeds the associated loaders.
        :param extraction_results:
        :return: None
        """
        for op in self.fanout:
            if isinstance(op, BaseExtractor):
                result = extraction_results[op]
                transformers = self.fanout.get(op, [])
                # TODO: parallelize this using joblib or something like that
                for transformer in transformers:
                    loaders = self.fanout.get(transformer, [])
                    for df in transformer.transform(result):  # type: pd.DataFrame
                        self._fire_loaders(df, loaders)
                    for loader in loaders:
                        loader.finalize()

    @staticmethod
    def _fire_loaders(df, loaders):
        # we consume a data frame as expected
        if isinstance(df, pd.DataFrame):
            if not df.empty:
                for loader in loaders:  # type: BaseLoader
                    loader.load(df)
        else:
            # we'll do our best to handle this and trust
            # that you configured the loader correctly
            # but we'll warn you
            from warnings import warn
            for loader in loaders:
                warn(f'Loading data of type {type(df)} that is not a DataFrame with {type(loader)}')
                loader.load(df)
