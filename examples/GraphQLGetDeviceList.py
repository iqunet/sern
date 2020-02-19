# requires iQunet version > 1.2.2
# install gql from github:
# (pip install -e git+git://github.com/graphql-python/gql.git#egg=gql)

import logging
from urllib.parse import urlparse
import time

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests


class GraphQLClient(object):
    CONNECT_TIMEOUT = 15  # [sec]
    RETRY_DELAY = 10  # [sec]
    MAX_RETRIES = 3  # [-]

    class Decorators(object):
        @staticmethod
        def autoConnectingClient(wrappedMethod):
            def wrapper(obj, *args, **kwargs):
                for retry in range(GraphQLClient.MAX_RETRIES):
                    try:
                        return wrappedMethod(obj, *args, **kwargs)
                    except Exception:
                        pass
                    try:
                        obj._logger.warn('(Re)connecting to GraphQL service.')
                        obj.reconnect()
                    except ConnectionRefusedError:
                        obj._logger.warn(
                            'Connection refused. Retry in 10s.'.format(
                                GraphQLClient.RETRY_DELAY
                            )
                        )
                        time.sleep(GraphQLClient.RETRY_DELAY)
                else:  # So the exception is exposed.
                    obj.reconnect()
                    return wrappedMethod(obj, *args, **kwargs)
            return wrapper

    def __init__(self, serverUrl):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.connect(
            serverUrl.geturl()
        )

    def __enter__(self):
        self.connect(
            serverUrl.geturl()
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._client = None

    def connect(self, url):
        host = url.split('//')[1].split('/')[0]
        request = requests.get(url,
                               headers={
                                       'Host': str(host),
                                       'Accept': 'text/html',
                                       }
                               )
        request.raise_for_status()
        csrf = request.cookies['csrftoken']
        self._client = Client(
                transport=RequestsHTTPTransport(url=url,
                                                cookies={"csrftoken": csrf},
                                                headers={'x-csrftoken':  csrf}
                                                ),
                fetch_schema_from_transport=True
                )

    def disconnect(self):
        self._client = None

    def reconnect(self):
        self.disconnect()
        self.connect(
            serverUrl.geturl()
        )

    @Decorators.autoConnectingClient
    def execute_query(self, querytext):
        query = gql(querytext)
        return self._client.execute(query)


class DataAcquisition(object):
    LOGGER = logging.getLogger('DataAcquisition')

    @staticmethod
    def get_device_list(serverUrl):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
            {
                deviceManager {
                    deviceList {
                        parent macId tag
                    }
                }
            }
            '''
            deviceList = client.execute_query(querytext)
            return deviceList


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("graphql").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    serverIP = "xx.xx.xx.xx"
    serverUrl = urlparse('http://{:s}:8000/graphql'.format(serverIP))
    deviceList = DataAcquisition.get_device_list(serverUrl=serverUrl)
    print(deviceList)
