import requests

from marshmallow import Schema
from typing import Optional
from urllib.parse import urljoin


# does this need to be separate from the model?

class RestMixin:

    def get(self, base_url, resource, params=None, data_schema: Schema = None, result_schema: Schema = None, **kwargs):

        url = urljoin(base_url, resource)
        return self._execute_request(url, 'GET', params=params, data_schema=data_schema,
                                     result_schema=result_schema, **kwargs)

    def post(self, base_url, resource, data=None, json=None, data_schema: Optional[Schema] = None,
             result_schema: Optional[Schema] = None, **kwargs):

        url = urljoin(base_url, resource)
        return self._execute_request(url, 'POST', data=data, json=json,
                                     data_schema=data_schema, result_schema=result_schema, **kwargs)

    def put(self, base_url, resource, data=None, json=None, data_schema: Optional[Schema] = None,
            result_schema: Optional[Schema] = None, **kwargs):

        url = urljoin(base_url, resource)
        return self._execute_request(url, 'PUT', data=data, json=json,
                                     data_schema=data_schema, result_schema=result_schema, **kwargs)

    @staticmethod
    def _execute_request(url, method: str, params=None, data=None, json=None,
                         data_schema: Optional[Schema] = None, result_schema: Optional[Schema] = None, **kwargs):

        if method.upper() == 'GET':
            if params and data_schema:
                params = data_schema.load(params)
            result = requests.get(url, params, **kwargs)
        elif method.upper() == 'POST':
            if data_schema and json:
                json = data_schema.load(json)
            result = requests.post(url, data=data, json=json, **kwargs)
        elif method.upper() == 'PUT':
            if data_schema and json:
                json = data_schema.load(json)
            result = requests.put(url, data=data, json=json, **kwargs)
        else:
            raise ValueError(f'Unsupported method: {method}')

        json_result = result.json()
        if result_schema:
            return result_schema.load(data=json_result)
        else:
            return json_result
