import pytest

from marshmallow import Schema, fields, ValidationError
from unittest import mock

from ketl.extractor.Rest import RestMixin


class JsonParams(Schema):

    param1 = fields.Int()
    param2 = fields.Str()


class JsonResponse(Schema):

    foo = fields.Str()
    bar = fields.Int()


@pytest.fixture
def rest():

    return RestMixin()


@pytest.fixture
def good_json_response():

    return {'foo': 'hello', 'bar': 1}


@pytest.fixture
def bad_json_response():

    return {'foo': 1, 'bar': 'hello'}


@mock.patch('ketl.extractor.Rest.requests')
def test_rest_get(mock_requests, good_json_response, bad_json_response, rest):

    mock_json_response = mock.Mock()
    mock_json_response.json.return_value = good_json_response
    mock_requests.get.return_value = mock_json_response

    result = rest.get('http://foo', 'bar', params={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                      result_schema=JsonResponse())

    assert result == good_json_response

    result = rest.get('http://foo', 'bar', params={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams())

    assert result == good_json_response

    mock_json_response.json.return_value = bad_json_response

    with pytest.raises(ValidationError):
        result = rest.get('http://foo', 'bar', params={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                          result_schema=JsonResponse())


@mock.patch('ketl.extractor.Rest.requests')
def test_rest_post(mock_requests, good_json_response, bad_json_response, rest):

    mock_json_response = mock.Mock()
    mock_json_response.json.return_value = good_json_response
    mock_requests.post.return_value = mock_json_response

    result = rest.post('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                       result_schema=JsonResponse())

    assert result == good_json_response

    result = rest.post('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams())

    assert result == good_json_response

    mock_json_response.json.return_value = bad_json_response

    with pytest.raises(ValidationError):
        result = rest.post('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                           result_schema=JsonResponse())


@mock.patch('ketl.extractor.Rest.requests')
def test_rest_put(mock_requests, good_json_response, bad_json_response, rest):

    mock_json_response = mock.Mock()
    mock_json_response.json.return_value = good_json_response
    mock_requests.put.return_value = mock_json_response

    result = rest.put('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                      result_schema=JsonResponse())

    assert result == good_json_response

    result = rest.put('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams())

    assert result == good_json_response

    mock_json_response.json.return_value = bad_json_response

    with pytest.raises(ValidationError):
        result = rest.put('http://foo', 'bar', json={'param1': 1, 'param2': 'foo'}, data_schema=JsonParams(),
                          result_schema=JsonResponse())


def test_unknown_method(rest):

    with pytest.raises(ValueError):
        rest._execute_request('http://foo', 'delete')
