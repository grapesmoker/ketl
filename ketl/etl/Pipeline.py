from typing import Dict, List, Callable, Optional, Union, Type
from itertools import product
from pathlib import Path
from collections import defaultdict

from ketl.extractor.Extractor import BaseExtractor
from ketl.transformer.Transformer import BaseTransformer
from ketl.loader.Loader import BaseLoader


class InvalidPipeline(Exception):

    pass


class ETLPipeline:

    def __init__(self, extractors: List[BaseExtractor] = None,
                 transformers: List[BaseTransformer] = None,
                 loaders: List[BaseLoader] = None,
                 fanout: Dict[Union[BaseExtractor, BaseTransformer, BaseLoader],
                              Union[List[BaseTransformer], List[BaseLoader]]] = None):

        if not fanout:
            self.fanout: Dict[Union[BaseExtractor, BaseTransformer, BaseLoader],
                              Union[List[BaseTransformer], List[BaseLoader]]] = defaultdict(list)
            for extractor, transformer in product(extractors, transformers):
                self.fanout[extractor].append(transformer)
            for transformer, loader in product(transformers, loaders):
                self.fanout[transformer].append(loader)
        else:
            for k, v in self.fanout.items():
                if isinstance(k, BaseExtractor) and not isinstance(v, BaseTransformer):
                    raise InvalidPipeline(f'An Extractor {k} is attached to something other than a Transformer: {v}.')
                elif k is BaseTransformer and not isinstance(v, BaseLoader):
                    raise InvalidPipeline(f'A Transformer {k} is attached to something other than a Loader: {v}.')
            self.fanout = fanout

    def execute(self):

        pass

    def _fire_extractors(self) -> Dict[BaseExtractor, List[Path]]:

        results = {op: op.extract() for op in self.fanout if isinstance(op, BaseExtractor)}

        return results

    def _fire_transformers(self, extraction_results: Dict[BaseExtractor, List[Path]]):

        for op in self.fanout:
            if isinstance(op, BaseExtractor):
                result = extraction_results[op]
                transformers = self.fanout[op]
                # TODO: parallelize this using joblib or something like that
                for transformer in transformers:
                    loaders = self.fanout[transformer]
                    for df in transformer.transform(result):
                        for loader in loaders:  # type: BaseLoader
                            loader.load(df)