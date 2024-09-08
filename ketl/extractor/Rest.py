import requests

from marshmallow import Schema
from typing import Optional
from urllib.parse import urljoin


class RestMixin:
    """ A mixin that contains calls that use a REST API.
    """
    def get(self, base_url, resource, params=None, data_schema: Schema = None, result_schema: Schema = None, **kwargs):
        """ Get a resource.

        :param base_url: the URL.
        :param resource: the resource to get (gets URL/resource).
        :param params: optional URL parameters.
        :param data_schema: an optional schema to validate submitted data.
        :param result_schema: an optional schema to validate returned data.
        :param kwargs: additional keyword args.
        :return: a JSON result.
        """
        url = urljoin(base_url, resource)
        return self._execute_request(url, 'GET', params=params, data_schema=data_schema,
                                     result_schema=result_schema, **kwargs)

    def post(self, base_url, resource, data=None, json=None, data_schema: Optional[Schema] = None,
             result_schema: Optional[Schema] = None, **kwargs):
        """ Post a resource.

        :param base_url: the URL.
        :param resource: the resource to get (gets URL/resource).
        :param params: optional URL parameters.
        :param data_schema: an optional schema to validate submitted data.
        :param result_schema: an optional schema to validate returned data.
        :param kwargs: additional keyword args.
        :return: a JSON result.
        """
        url = urljoin(base_url, resource)
        return self._execute_request(url, 'POST', data=data, json=json,
                                     data_schema=data_schema, result_schema=result_schema, **kwargs)

    def put(self, base_url, resource, data=None, json=None, data_schema: Optional[Schema] = None,
            result_schema: Optional[Schema] = None, **kwargs):
        """ Put a resource.

        :param base_url: the URL.
        :param resource: the resource to get (gets URL/resource).
        :param params: optional URL parameters.
        :param data_schema: an optional schema to validate submitted data.
        :param result_schema: an optional schema to validate returned data.
        :param kwargs: additional keyword args.
        :return: a JSON result.
        """
        url = urljoin(base_url, resource)
        return self._execute_request(url, 'PUT', data=data, json=json,
                                     data_schema=data_schema, result_schema=result_schema, **kwargs)

    @staticmethod
    def _execute_request(url, method: str, params=None, data=None, json=None,
                         data_schema: Optional[Schema] = None, result_schema: Optional[Schema] = None, **kwargs):
        """ Execute a request.

        :param url: the URL.
        :param method: the method to request.
        :param params: optional URL parameters.
        :param data: optional data to put or post.
        :param data_schema: an optional schema to validate submitted data.
        :param result_schema: an optional schema to validate returned data.
        :param kwargs: additional keyword args.
        :return: a JSON result.
        """
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

        if result.ok:
            json_result = result.json()
            if result_schema:
                return result_schema.load(data=json_result)
            else:
                return json_result
        else:
            result.raise_for_status()
