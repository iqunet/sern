# requires iQunet version > 1.2.2
# install gql from github:
# (pip install -e git+git://github.com/graphql-python/gql.git#egg=gql)

import logging
from urllib.parse import urlparse

import time
import datetime
from dateutil import parser
import pytz

import matplotlib.pyplot as plt
import numpy as np
import scipy.signal

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests

class HighPassFilter(object):

    @staticmethod
    def get_highpass_coefficients(lowcut, sampleRate, order=5):
        nyq = 0.5 * sampleRate
        low = lowcut / nyq
        b, a = scipy.signal.butter(order, [low], btype='highpass')
        return b, a

    @staticmethod
    def run_highpass_filter(data, lowcut, sampleRate, order=5):
        if lowcut >= sampleRate/2.0:
            return data*0.0
        b, a = HighPassFilter.get_highpass_coefficients(lowcut, sampleRate, order=order)
        y = scipy.signal.filtfilt(b, a, data, padtype='even')
        return y
    
    @staticmethod
    def perform_hpf_filtering(data, sampleRate, hpf=3):
        if hpf == 0:
            return data
        data[0:6] = data[13:7:-1] # skip compressor settling
        data = HighPassFilter.run_highpass_filter(
            data=data,
            lowcut=3,
            sampleRate=sampleRate,
            order=1,
        )
        data = HighPassFilter.run_highpass_filter(
            data=data,
            lowcut=int(hpf),
            sampleRate=sampleRate,
            order=2,
        )
        return data

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
                        obj._logger.warning(
                                '(Re)connecting to GraphQL service.'
                        )
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
    def get_sensor_data(serverUrl, macId, starttime, endtime, limit, axis):
        with GraphQLClient(serverUrl) as client:
            querytext = '''
			{ deviceManager { device(macId:"''' + macId + '''") {
                __typename
                ... on GrapheneVibrationCombo {vibrationTimestampHistory(start:"''' + str(starttime) + '''", end:"''' + str(endtime) + '''", limit:''' + str(limit) + ''', axis:"''' + axis + '''")}
            }}}
            '''
            result = client.execute_query(querytext)
            times = \
                result['deviceManager']['device']['vibrationTimestampHistory']
            dates, values, fRanges, numSamples, sampleRates = ([], [], [], [], [])
            for t in times:
                result = DataAcquisition.get_sensor_measurement(
                        client,
                        macId,
                        t
                )
                dates.append(t)
                deviceData = result['deviceManager']['device']
                values.append(
                        deviceData['vibrationArray']['rawSamples']
                )
                fRanges.append(
                        deviceData['vibrationArray']['formatRange']
                )
                numSamples.append(
                        deviceData['vibrationArray']['numSamples']
                )
                sampleRates.append(
                        deviceData['vibrationArray']['sampleRate']
                )
            return (values, dates, fRanges, numSamples, sampleRates)

    @staticmethod
    def get_sensor_measurement(client, macId, isoDate):
        querytext = '''
        { deviceManager { device(macId:"''' + macId + '''") {
        __typename
        ... on GrapheneVibrationCombo { vibrationArray(isoDate: "''' + isoDate + '''") {
        numSamples rawSamples sampleRate formatRange axis }}
        }}}
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

    # change settings
    hpf = 3 # high pass filter (Hz)
    startTime = "2021-02-01"
    endTime = "2021-02-24"
    timeZone = "Europe/Brussels" # local time zone
    limit = 6 # limit limits the number of returned measurements
    axis = 'XYZ'  # axis allows to select data from only 1 or multiple axes

    # acquire history data
    (values, dates, fRanges, numSamples, sampleRates) = DataAcquisition.get_sensor_data(
            serverUrl=serverUrl,
            macId=macId,
            starttime=startTime,
            endtime=endTime,
            limit=limit,
            axis=axis
    )

    # convert vibration data to 'g' units and plot data
    for i in range(len(fRanges)):
        values[i] = [d/512.0*fRanges[i] for d in values[i]]
        maxTimeValue = numSamples[i]/sampleRates[i]
        stepSize = 1/sampleRates[i]
        timeValues = np.arange(0, maxTimeValue, stepSize)
        
        values[i] = HighPassFilter.perform_hpf_filtering(
            data=values[i],
            sampleRate=sampleRates[i], 
            hpf=hpf
        )
        
        plt.figure()
        plt.plot(timeValues, values[i])
        title = parser.parse(dates[i]).astimezone(pytz.timezone(timeZone))
        title = (title + datetime.timedelta(seconds=.5)).replace(microsecond=0)
        title = title.strftime("%a %b %d %Y %H:%M:%S")
        plt.title(title)
        plt.xlim((0, maxTimeValue)) 
        plt.xlabel('Time [s]')
        plt.ylabel('RMS Acceleration [g]')
        
