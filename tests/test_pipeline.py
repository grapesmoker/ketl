import pytest

from unittest import mock
from pathlib import Path

from ketl.db.settings import get_session
from ketl.etl.Pipeline import ETLPipeline, InvalidPipelineError
from ketl.extractor.Extractor import DefaultExtractor
from ketl.transformer.Transformer import DelimitedTableTransformer
from ketl.loader.Loader import DelimitedFileLoader
from tests.factories import APIFactory


def test_pipeline_init_without_fanout():

    api = APIFactory(name='my nice api')

    ex1 = DefaultExtractor(api)
    ex2 = DefaultExtractor(api)
    tf1 = DelimitedTableTransformer()
    tf2 = DelimitedTableTransformer()
    l1 = DelimitedFileLoader('out.csv')
    l2 = DelimitedFileLoader('out.csv')

    pipeline = ETLPipeline(extractors=[ex1, ex2], transformers=[tf1, tf2], loaders=[l1, l2])

    for k, v in pipeline.fanout.items():
        if k == ex1:
            assert pipeline.fanout[k] == [tf1, tf2]
        if k == ex2:
            assert pipeline.fanout[k] == [tf1, tf2]
        if k == tf1:
            assert pipeline.fanout[k] == [l1, l2]
        if k == tf2:
            assert pipeline.fanout[k] == [l1, l2]


def test_pipeline_init_with_fanout(tmp_path):

    api = APIFactory(name='my nice api')

    ex1 = DefaultExtractor(api)
    ex2 = DefaultExtractor(api)
    tf1 = DelimitedTableTransformer()
    tf2 = DelimitedTableTransformer()
    l1 = DelimitedFileLoader('out.csv')
    l2 = DelimitedFileLoader('out.csv')

    fanout = {
        ex1: [tf1, tf2],
        tf1: [l1],
        tf2: [l2]
    }

    pipeline = ETLPipeline(extractors=[ex1, ex2], transformers=[tf1, tf2], loaders=[l1, l2], fanout=fanout)

    assert pipeline.fanout == fanout


def test_pipeline_init_bad_fanout():

    api = APIFactory(name='my nice api')

    ex1 = DefaultExtractor(api)
    ex2 = DefaultExtractor(api)
    tf1 = DelimitedTableTransformer()
    tf2 = DelimitedTableTransformer()
    l1 = DelimitedFileLoader('out.csv')
    l2 = DelimitedFileLoader('out.csv')

    fanout = {
        ex1: [tf1, l1],
        tf1: [l1, l2],
        tf2: [l2]
    }

    with pytest.raises(InvalidPipelineError):
        pipeline = ETLPipeline(fanout=fanout)

    fanout = {
        ex1: [tf1, tf2],
        tf1: [l1, l2],
        tf2: [tf2]
    }

    with pytest.raises(InvalidPipelineError):
        pipeline = ETLPipeline(fanout=fanout)


@mock.patch('ketl.extractor.Extractor.DefaultExtractor.extract')
def test_fire_extractors(mock_extract):

    api = APIFactory(name='my nice api')

    ex1 = DefaultExtractor(api)
    ex2 = DefaultExtractor(api)
    tf1 = DelimitedTableTransformer()
    tf2 = DelimitedTableTransformer()

    pipeline = ETLPipeline(extractors=[ex1, ex2], transformers=[tf1, tf2])

    mock_extract.side_effect = [[Path('file1'), Path('file2')], [Path('file3')]]

    result = pipeline._fire_extractors()

    assert result == {ex1: [Path('file1'), Path('file2')], ex2: [Path('file3')]}


@mock.patch('ketl.loader.Loader.DelimitedFileLoader.load')
@mock.patch('ketl.transformer.Transformer.DelimitedTableTransformer.transform')
def test_fire_transformers(mock_transform: mock.Mock, mock_load: mock.Mock):

    api = APIFactory(name='my nice api')

    ex1 = DefaultExtractor(api)
    tf1 = DelimitedTableTransformer()
    l1 = DelimitedFileLoader('out.csv')

    pipeline = ETLPipeline(extractors=[ex1], transformers=[tf1], loaders=[l1])

    extraction_results = {ex1: [Path('file1')]}

    mock_df = mock.Mock()
    mock_df.empty = False
    mock_transform.return_value = [mock_df]
    mock_load.return_value = None

    pipeline._fire_transformers(extraction_results)

    mock_transform.assert_called_once()
    mock_load.assert_called_once()
