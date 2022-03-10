import sys
import pytz
import logging
import datetime
import os
import matplotlib.pyplot as plt
import numpy as np
import scipy.signal
import math
import matplotlib.dates as mdates

import asyncio
from asyncua import Client
from asyncua import ua

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

class OpcUaClient(object):
    CONNECT_TIMEOUT = 15  # [sec]
    RETRY_DELAY = 10  # [sec]
    MAX_RETRIES = 3  # [-]

    class Decorators(object):
        @staticmethod
        def autoConnectingClient(wrappedMethod):
            async def wrapper(obj, *args, **kwargs):
                for retry in range(OpcUaClient.MAX_RETRIES):
                    try:
                        return await wrappedMethod(obj, *args, **kwargs)
                    except ua.uaerrors.BadNoMatch:
                        raise
                    except Exception:
                        pass
                    try:
                        obj._logger.warning('(Re)connecting to OPC-UA service.')
                        obj.reconnect()
                    except ConnectionRefusedError:
                        obj._logger.warning(
                            'Connection refused. Retry in 10s.'.format(
                                OpcUaClient.RETRY_DELAY
                            )
                        )
                        await asyncio.sleep(OpcUaClient.RETRY_DELAY)
                else:  # So the exception is exposed.
                    obj.reconnect()
                    return await wrappedMethod(obj, *args, **kwargs)
            return wrapper

    def __init__(self, serverUrl):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = Client(
            serverUrl,
            timeout=self.CONNECT_TIMEOUT
        )

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()
        self._client = None

    async def connect(self):
        await self._client.connect()
        await self._client.load_data_type_definitions()

    async def disconnect(self):
        try:
            await self._client.disconnect()
        except Exception:
            pass

    async def reconnect(self):
        await self.disconnect()
        await self.connect()
    
    @Decorators.autoConnectingClient
    async def read_browse_name(self, uaNode):
        return await uaNode.read_browse_name()

    @Decorators.autoConnectingClient
    async def read_node_class(self, uaNode):
        return await uaNode.read_node_class()

    @Decorators.autoConnectingClient
    async def get_namespace_index(self, uri):
        return await self._client.get_namespace_index(uri)

    @Decorators.autoConnectingClient
    async def get_child(self, uaNode, path):
        return await uaNode.get_child(path)
    
    @Decorators.autoConnectingClient
    async def get_sensor_list(self):
        objectsNode = await self.get_objects_node()
        return await objectsNode.get_children()

    @Decorators.autoConnectingClient
    async def get_objects_node(self):
        path = [ua.QualifiedName('Objects', 0)]
        root = self._client.get_root_node()
        return await root.get_child(path)

    @Decorators.autoConnectingClient
    async def read_raw_history(self,
                                uaNode,
                                starttime=None,
                                endtime=None,
                                numvalues=0,
                                ):
        return await uaNode.read_raw_history(
            starttime=starttime,
            endtime=endtime,
            numvalues=numvalues,
        )
    
class DataAcquisition(object):
    LOGGER = logging.getLogger('DataAcquisition')
    
    @staticmethod
    async def get_sensor_data(serverUrl, macId, browseName, starttime, endtime):
        async with OpcUaClient(serverUrl) as client:
            sensorNode = await DataAcquisition.get_sensor_node(
                client,
                macId,
                browseName
            )
            DataAcquisition.LOGGER.info(
                    'Browsing {:s}'.format(macId)
            )
            (values, dates) = \
                await DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
        return (values, dates)
    
    @staticmethod
    async def get_sensor_node(client, macId, browseName):
        nsIdx = await client.get_namespace_index(
                'http://www.iqunet.com'
        )  # iQunet namespace index
        bpath = []
        bpath.append(ua.QualifiedName(macId, nsIdx))
        bpath.append(ua.QualifiedName(browseName, nsIdx))
        objectsNode = await client.get_objects_node()
        sensorNode = await objectsNode.get_child(bpath)
        return sensorNode
    
    @staticmethod
    async def get_endnode_data(client, endNode, starttime, endtime):
        dvList = await DataAcquisition.download_endnode(
            client=client,
            endNode=endNode,
            starttime=starttime,
            endtime=endtime
        )
        dates, values = ([], [])
        for dv in dvList:
            dates.append(dv.SourceTimestamp.strftime('%Y-%m-%d %H:%M:%S'))
            values.append(dv.Value.Value)

        # If no starttime is given, results of read_raw_history are reversed.
        if starttime is None:
            values.reverse()
            dates.reverse()
        return (values, dates)         
           
    @staticmethod
    async def download_endnode(client, endNode, starttime, endtime):
        endNodeName = await client.read_browse_name(endNode)
        DataAcquisition.LOGGER.info(
                'Downloading endnode {:s}'.format(
                        endNodeName.Name
                    )
        )
        dvList = await client.read_raw_history(
            uaNode=endNode,
            starttime=starttime,
            endtime=endtime,
        )
        if not len(dvList):
            DataAcquisition.LOGGER.warning(
                'No data was returned for {:s}'.format(endNodeName.Name)
            )
        else:
            sys.stdout.write('\r    Loaded {:d} values, {:s} -> {:s}'.format(
                len(dvList),
                str(dvList[0].ServerTimestamp.strftime("%Y-%m-%d %H:%M:%S")),
                str(dvList[-1].ServerTimestamp.strftime("%Y-%m-%d %H:%M:%S"))
            ))
            sys.stdout.flush()
            sys.stdout.write('...OK.\n')
        return dvList

async def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("opcua").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    url: str = 'opc.tcp://xx.xx.xx.xx:4840/freeopcua/server'

    # replace xx:xx:xx:xx with your sensors macId
    macId = 'xx:xx:xx:xx'
    
    # change settings
    hpf = 6 # high pass filter (Hz)
    startTime = "2022-02-11 00:00:00"
    endTime = "2022-02-15 10:00:00"
    timeZone = "Europe/Brussels" # local time zone
    
    # format start and end time
    starttime = pytz.utc.localize(
        datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
    )
    endtime = pytz.utc.localize(
        datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S')
    )
    
    # acquire history data
    (values, dates) = await DataAcquisition.get_sensor_data(
        serverUrl=url,
        macId=macId,
        browseName="accelerationPack",
        starttime=starttime,
        endtime=endtime
    )
    
    # create folder to save images
    cwd = os.getcwd()
    folder = cwd + "\Vibration"
    if os.path.isdir(folder):
        pass
    else:
        os.mkdir(folder)

    # convert vibration data to 'g' units and plot data
    data = [val[1:-6] for val in values]
    numSamples = [val[0] for val in values]
    sampleRates = [val[-6] for val in values]
    formatRanges = [val[-5] for val in values]
    axes = [val[-3] for val in values]
    for i in range(len(formatRanges)):
        data[i] = [d/512.0*formatRanges[i] for d in data[i]]
        maxTimeValue = numSamples[i]/sampleRates[i]
        stepSize = 1/sampleRates[i]
        timeValues = np.arange(0, maxTimeValue, stepSize)
        
        data[i] = HighPassFilter.perform_hpf_filtering(
            data=data[i],
            sampleRate=sampleRates[i], 
            hpf=hpf
        )
        
        # plot time domain
        plt.figure()
        plt.plot(timeValues, data[i])
        title = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
        title = title.replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
        title = title.strftime("%a %b %d %Y %H:%M:%S")        
        plt.title(title)
        plt.xlim((0, maxTimeValue)) 
        plt.xlabel('Time [s]')
        plt.ylabel('RMS Acceleration [g]')
        
        dateTitle = title.replace(" ", "_")
        dateTitle = dateTitle.replace(":","")
        figTitle = folder + '\image_' + dateTitle + '_' + str(i) + '_time.png'
        plt.savefig(figTitle)
        plt.close()
        
        #plot frequency domain
        plt.figure()
        windowSize = len(data[i]) # window size
        nOverlap   = 0 # overlap window
        windowType = 'hann' # hanning window     
        mode       = 'magnitudeRMS' # RMS magnitude spectrum.
        (npFFT, npFreqs) = FourierTransform.perform_fft_windowed(
            signal=data[i], 
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
        
        figTitle = folder + '\image_' + dateTitle + '_' + str(i) + '_freq.png'
        plt.savefig(figTitle)
        plt.close()
        
    # acquire board temperature
    (temperatures, dates) = await DataAcquisition.get_sensor_data(
        serverUrl=url,
        macId=macId,
        browseName="boardTemperature",
        starttime=starttime,
        endtime=endtime
    )
    for i in range(len(dates)):
          dates[i] = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
          dates[i] = dates[i].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
    plt.figure()
    plt.plot(dates, temperatures)
    plt.gcf().autofmt_xdate()
    myFmt = mdates.DateFormatter('%Y-%m-%d %H:%M')
    plt.gca().xaxis.set_major_formatter(myFmt)
    plt.title('Board Temperature')
    plt.ylabel('°C')
    
    figTitle = folder + '\Boardtemperature.png'
    plt.savefig(figTitle)
    plt.close()

if __name__ == '__main__':
    asyncio.run(main())
