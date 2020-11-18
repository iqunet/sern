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


class DataMutation(object):
    LOGGER = logging.getLogger('DataMutation')

    @staticmethod
    def reboot_sensor(serverUrl, macId):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
            mutation {
                reboot(macId:"''' + macId + '''"){
                    ok
                    }
                }
            '''
            return client.execute_query(querytext)
        
    @staticmethod
    def set_sample_rate(serverUrl, macId, sampleRate):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
            mutation {
                setSampleRate(macId:"''' + macId + '''",sampleRate:''' + str(sampleRate) + '''){
                    ok
                    }
                }
            '''
            return client.execute_query(querytext)

    @staticmethod
    def set_num_samples(serverUrl, macId, numSamples):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
            mutation {
                setNumSamples(numSamples:''' + str(numSamples) + ''',macId:"''' + macId + '''"){
                    ok
                    }
                }
            '''
            return client.execute_query(querytext)

    @staticmethod
    def start_vibration_measurement(serverUrl, macId, hpf, prefetch, sampleRate, formatRange, threshold, axis, numSamples):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
            mutation {
                vibrationRunSetup(hpf:''' + str(hpf) + ''',prefetch:''' + str(prefetch) + ''',sampleRate:''' + str(sampleRate) + ''',formatRange:''' + str(formatRange) + ''',threshold:''' + str(threshold) + ''',axis:"''' + axis + '''", numSamples:''' + str(numSamples) + ''', macId:"''' + macId + '''"){
                    ok
                    }
                }
            '''
            return client.execute_query(querytext)

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("graphql").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    serverIP = "xx.xx.xx.xx"
    serverUrl = urlparse('http://{:s}:8000/graphql'.format(serverIP))

    # replace xx:xx:xx:xx with your sensors macId
    macId = 'xx:xx:xx:xx'

    # reboot sensor
    result = DataMutation.reboot_sensor(
            serverUrl=serverUrl,
            macId=macId,
    )
    print(result)
    
    # set sample rate
    result = DataMutation.set_sample_rate(
            serverUrl=serverUrl,
            macId=macId,
            sampleRate=800
    )
    print(result)
    
    # set number of samples
    result = DataMutation.set_num_samples(
            serverUrl=serverUrl,
            macId=macId,
            numSamples=1024
    )
    print(result)
    
    # start vibration measurement
    result = DataMutation.start_vibration_measurement(
            serverUrl=serverUrl,
            macId=macId,
            hpf = 3,
            prefetch = 64,
            sampleRate = 3200,
            formatRange = 16,
            threshold = 0,
            axis = "XYZ",
            numSamples=1024
    )
    print(result)
