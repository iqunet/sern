import sys
import pytz
import logging
import datetime
import os
import matplotlib.pyplot as plt
import numpy as np
import scipy.signal

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
    selectedAxes = [0, 1] # select 2 axes (X=0, Y=1, Z=2)
    # only select the two first chosen axes
    if len(selectedAxes) > 2:
        selectedAxes = selectedAxes[0:2]
    
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

    # convert vibration data to 'g' units and plot data if axis in selected axes list
    data = [val[1:-6] for val in values]
    sampleRates = [val[-6] for val in values]
    formatRanges = [val[-5] for val in values]
    axes = [val[-3] for val in values]
    
    selectedData = []
    selectedDates = []
    for i in range(len(formatRanges)):
        if axes[i] in selectedAxes:
            data[i] = [d/512.0*formatRanges[i] for d in data[i]]
            data[i] = HighPassFilter.perform_hpf_filtering(
                data=data[i],
                sampleRate=sampleRates[i], 
                hpf=hpf
            )
            selectedData.append(data[i])
            selectedDates.append(datetime.datetime.strptime(dates[i], '%Y-%m-%d %H:%M:%S'))
          
    # sort matching axes pairs
    duration = [((selectedDates[i]-selectedDates[i-1]).total_seconds())*1000 for i in range(1,len(selectedDates))]
    duration = np.array(duration)
    idx = np.where(duration < 10)[0]
    pairs = [[ind, ind+1] for ind in idx]
    
    # plot matching axes pairs
    axesTitle = str(selectedAxes[0]) + "-" + str(selectedAxes[1])
    axesTitle = axesTitle.replace('0','X')
    axesTitle = axesTitle.replace('1','Y')
    axesTitle = axesTitle.replace('2','Z')
    
    for p in pairs:
        if len(selectedData[p[0]]) == len(selectedData[p[1]]):
            plt.figure()
            plt.plot(selectedData[p[0]],selectedData[p[1]])
            dateTitle = selectedDates[p[0]].replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timeZone))
            dateTitle = dateTitle.strftime("%a, %b %d, %Y %I:%M %p")
            plt.suptitle("ACC | SCATTER " + axesTitle, fontsize=12)
            title = dateTitle + " | axis: " + axesTitle
            plt.title(title, fontsize=10)
            plt.xlabel(axesTitle[0] + ' [g]')
            plt.ylabel(axesTitle[2] + ' [g]')
            
            title = title.replace("|", "")
            title = title.replace(":", "")
            title = title.replace(" ", "_")
            title = title.replace(",", "")
            figTitle = folder + '\orbit_' + title + '.png'
            plt.savefig(figTitle)
            plt.close()
        else:
            pass

if __name__ == '__main__':
    asyncio.run(main())
