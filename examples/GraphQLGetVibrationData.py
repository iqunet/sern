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
import math
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
    
class FourierTransform(object):

    @staticmethod
    def perform_fft_windowed(signal, fs, winSize, nOverlap, window, detrend = True, mode = 'lin'):
        assert(nOverlap < winSize)
        assert(mode in ('magnitudeRMS', 'magnitudePeak', 'lin', 'log'))
    
        # Compose window and calculate 'coherent gain scale factor'
        w = scipy.signal.get_window(window, winSize)
        # http://www.bores.com/courses/advanced/windows/files/windows.pdf
        # Bores signal processing: "FFT window functions: Limits on FFT analysis"
        # F. J. Harris, "On the use of windows for harmonic analysis with the
        # discrete Fourier transform," in Proceedings of the IEEE, vol. 66, no. 1,
        # pp. 51-83, Jan. 1978.
        coherentGainScaleFactor = np.sum(w)/winSize
    
        # Zero-pad signal if smaller than window
        padding = len(w) - len(signal)
        if padding > 0:
            signal = np.pad(signal, (0,padding), 'constant')
    
        # Number of windows
        k = int(np.fix((len(signal)-nOverlap)/(len(w)-nOverlap)))
    
        # Calculate psd
        j = 0
        spec = np.zeros(len(w));
        for i in range(0, k):
            segment = signal[j:j+len(w)]
            if detrend is True:
                segment = scipy.signal.detrend(segment)
            winData = segment*w
            # Calculate FFT, divide by sqrt(N) for power conservation,
            # and another sqrt(N) for RMS amplitude spectrum.
            fftData = np.fft.fft(winData, len(w))/len(w)
            sqAbsFFT = abs(fftData/coherentGainScaleFactor)**2
            spec = spec + sqAbsFFT;
            j = j + len(w) - nOverlap
    
        # Scale for number of windows
        spec = spec/k
    
        # If signal is not complex, select first half
        if len(np.where(np.iscomplex(signal))[0]) == 0:
            stop = int(math.ceil(len(w)/2.0))
            # Multiply by 2, except for DC and fmax. It is asserted that N is even.
            spec[1:stop-1] = 2*spec[1:stop-1]
        else:
            stop = len(w)
        spec = spec[0:stop]
        freq = np.round(float(fs)/len(w)*np.arange(0, stop), 2)
    
        if mode == 'lin': # Linear Power spectrum
            return (spec, freq)
        elif mode == 'log': # Log Power spectrum
            return (10.*np.log10(spec), freq)
        elif mode == 'magnitudeRMS': # RMS Magnitude spectrum
            return (np.sqrt(spec), freq)
        elif mode == 'magnitudePeak': # Peak Magnitude spectrum
            return (np.sqrt(2.*spec), freq)

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
    
    @staticmethod
    def get_temperature_data(serverUrl, macId, timeZone):
        with GraphQLClient(serverUrl) as client:
            result = DataAcquisition.get_temperature_measurement(
                    client,
                    macId
            )
            tz = pytz.timezone(timeZone) 
            date = datetime.datetime.now(tz)
            date = date.strftime("%a %b %d %Y %H:%M:%S")
            deviceData = result['deviceManager']['device']
            temperature = deviceData['temperature']
            return (date, temperature)
    
    @staticmethod
    def get_temperature_measurement(client, macId):
        querytext = '''
        { deviceManager { device(macId:"''' + macId + '''") {
        __typename
        ... on GrapheneVibrationCombo { temperature }
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
    startTime = "2021-02-10"
    endTime = "2021-02-24"
    timeZone = "Europe/Brussels" # local time zone
    limit = 4 # limit limits the number of returned measurements
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
        
        # plot time domain
        plt.figure()
        plt.plot(timeValues, values[i])
        title = parser.parse(dates[i]).astimezone(pytz.timezone(timeZone))
        title = (title + datetime.timedelta(seconds=.5)).replace(microsecond=0)
        title = title.strftime("%a %b %d %Y %H:%M:%S")
        plt.title(title)
        plt.xlim((0, maxTimeValue)) 
        plt.xlabel('Time [s]')
        plt.ylabel('RMS Acceleration [g]')
        
        #plot frequency domain
        plt.figure()
        windowSize = len(values[i]) # window size
        nOverlap   = 0 # overlap window
        windowType = 'hann' # hanning window     
        mode       = 'magnitudeRMS' # RMS magnitude spectrum.
        (npFFT, npFreqs) = FourierTransform.perform_fft_windowed(
            signal=values[i], 
            fs=sampleRates[i],
            winSize=windowSize,
            nOverlap=nOverlap, 
            window=windowType, 
            detrend = False, 
            mode = mode)
        plt.plot(npFreqs, npFFT)
        plt.title(title)
        plt.xlim((0, sampleRates[i]/2)) 
        viewPortOptions = [0.1, 0.2, 0.5, 1, 2, 4, 8, 16]
        viewPort = [i for i in viewPortOptions if i >= max(npFFT)][0]
        plt.ylim((0,viewPort))
        plt.xlabel('Frequency [Hz]')
        plt.ylabel('RMS Acceleration [g]')
        
    # acquire board temperature
    (date, temperature) = DataAcquisition.get_temperature_data(
        serverUrl=serverUrl, 
        macId=macId, 
        timeZone=timeZone)
    print(date + ' : ' + str(temperature))
