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
    async def get_sensor_sub_node(client, macId, browseName, subBrowseName, sub2BrowseName=None, sub3BrowseName=None, sub4BrowseName=None):
        nsIdx = await client.get_namespace_index(
                'http://www.iqunet.com'
        )  # iQunet namespace index
        bpath = [
                ua.QualifiedName(macId, nsIdx),
                ua.QualifiedName(browseName, nsIdx),
                ua.QualifiedName(subBrowseName, nsIdx)
        ]
        if sub2BrowseName is not None:
            bpath.append(ua.QualifiedName(sub2BrowseName, nsIdx))
        if sub3BrowseName is not None:
            bpath.append(ua.QualifiedName(sub3BrowseName, nsIdx))
        if sub4BrowseName is not None:
            bpath.append(ua.QualifiedName(sub4BrowseName, nsIdx))
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
    
    @staticmethod
    async def get_anomaly_model_nodes(client, macId):
        sensorNode = \
            await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models")
        DataAcquisition.LOGGER.info(
                'Browsing for models of {:s}'.format(macId)
        )
        modelNodes = await sensorNode.get_children()
        return modelNodes
    
    @staticmethod
    async def get_anomaly_model_parameters(client, macId, starttime, endtime):
        modelNodes = \
            await DataAcquisition.get_anomaly_model_nodes(client, macId)
        models = dict()
        for mnode in modelNodes:
            key = await mnode.read_display_name()
            key = key.Text
            sensorNode = \
                 await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE")
            (valuesraw, datesraw) = \
                await DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_05pct")
            (values05, dates05) = \
                await DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_50pct")
            (values50, dates50) = \
                await DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_95pct")
            (values95, dates95) = \
                await DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                 await DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "alarmLevel")
            alarmLevel = await sensorNode.get_value()
            modelSet = {
                "raw": (valuesraw, datesraw),
                "expectile_05pct": (values05, dates05),
                "expectile_50pct": (values50, dates50),
                "expectile_95pct": (values95, dates95),
                "alarmLevel": alarmLevel
                }
            models[key] = modelSet
        return models

async def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("opcua").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    url: str = 'opc.tcp://xx.xx.xx.xx:4840/freeopcua/server'

    # replace xx:xx:xx:xx with your sensors macId
    macId = 'xx:xx:xx:xx'
    
    # change settings
    hpf = 6 # high pass filter (Hz)
    startTime = "2022-03-08 00:00:00"
    endTime = "2022-03-15 10:00:00"
    timeZone = "Europe/Brussels" # local time zone
    
    # format start and end time
    starttime = pytz.utc.localize(
        datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
    )
    endtime = pytz.utc.localize(
        datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S')
    )
    
    # create folder to save images
    cwd = os.getcwd()
    folder = cwd + "\Vibration"
    if os.path.isdir(folder):
        pass
    else:
        os.mkdir(folder)
    
    # create opc ua client
    async with OpcUaClient(url) as client:
        # acquire model data
        modelDict = await DataAcquisition.get_anomaly_model_parameters(
            client=client,
            macId=macId,
            starttime=starttime,
            endtime=endtime
        )
        for model in modelDict.keys():
            plt.figure()

            dates = modelDict[model]["raw"][1]
            for i in range(len(dates)):
                dates[i] = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
                dates[i] = dates[i].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
            plt.plot(dates, modelDict[model]["raw"][0], label='raw')
            
            dates = modelDict[model]["expectile_05pct"][1]
            for i in range(len(dates)):
                dates[i] = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
                dates[i] = dates[i].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
            plt.plot(dates, modelDict[model]["expectile_05pct"][0],'b', label = 'LO 5%')
            
            dates = modelDict[model]["expectile_50pct"][1]
            for i in range(len(dates)):
                dates[i] = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
                dates[i] = dates[i].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
            plt.plot(dates, modelDict[model]["expectile_50pct"][0],'r', label = 'median')
            
            dates = modelDict[model]["expectile_95pct"][1]
            for i in range(len(dates)):
                dates[i] = datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S')
                dates[i] = dates[i].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
            plt.plot(dates, modelDict[model]["expectile_95pct"][0],'b', label = 'HI 95%')
            
            plt.gcf().autofmt_xdate()
            myFmt = mdates.DateFormatter('%Y-%m')
            plt.gca().xaxis.set_major_formatter(myFmt)
            plt.title('Vibration Anomaly (' + model + ')')
            plt.ylabel('Pred. Error [-]')
            
            alarmLevel = modelDict[model]["alarmLevel"]
            plt.axhline(y=alarmLevel, color='r')
            
            plt.legend(loc="upper left")
            
            figTitle = folder + '\Anomaly_' + str(model) + '.png'
            plt.savefig(figTitle)
            plt.close()

if __name__ == '__main__':
    asyncio.run(main())
