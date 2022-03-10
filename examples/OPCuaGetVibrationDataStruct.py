import sys
import pytz
import logging
import datetime
import itertools
import os
import matplotlib.pyplot as plt

import asyncio
from asyncua import Client
from asyncua import ua


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
    AXES = ('x', 'y', 'z')
    ORDINATES = ('accel', 'veloc')
    DOMAINS = ('time', 'freq')

    @staticmethod
    async def get_sensor_data(serverUrl, macId, browseName, starttime, endtime):
        allXValues = []
        allYValues = []
        allDates = []
        allAxes = []
        async with OpcUaClient(serverUrl) as client:
            sensorNode = await DataAcquisition.get_sensor_node(
                client,
                macId,
                browseName
            )
            for path in DataAcquisition.endnodes_path_generator(sensorNode):
                name = await sensorNode.read_browse_name()
                DataAcquisition.LOGGER.info(
                        'Browsing {:s} -> {:s}'.format(
                                macId,
                                name.Name
                            )
                )
                endNode = await client.get_child(sensorNode, path)
                (xvalues, yvalues, dates, axes) = await DataAcquisition.get_endnode_data(
                    client,
                    endNode,
                    starttime,
                    endtime
                )
                allXValues.extend(xvalues)
                allYValues.extend(yvalues)
                allDates.extend(dates)
                allAxes.extend(axes)
        return (allXValues, allYValues, allDates, allAxes)

    @staticmethod
    def endnodes_path_generator(sensorNode):
        for (axis, ordinate, domain) in \
                itertools.product(DataAcquisition.AXES,
                                  DataAcquisition.ORDINATES,
                                  DataAcquisition.DOMAINS):
            # browseName: e.g. xAccelTime
            browseName = ''.join([
                axis, ordinate.capitalize(), domain.capitalize()
            ])
            nsIdx = sensorNode.nodeid.NamespaceIndex  # iQunet namespace index
            path = [
                ua.QualifiedName(axis, nsIdx),        # e.g. 'x'
                ua.QualifiedName(ordinate, nsIdx),    # e.g. 'accel'
                ua.QualifiedName(browseName, nsIdx),  # e.g. 'xAccelTime'
            ]
            yield path

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
            client,
            endNode,
            starttime,
            endtime
        )
        dates, yvalues, xvalues, axes = ([], [], [], [])
        for dv in dvList:
            dates.append(dv.SourceTimestamp.strftime('%Y-%m-%d %H:%M:%S'))
            yvalues.append(dv.Value.Value.y_ordinate)
            xvalues.append(dv.Value.Value.x_abscissa)
            axes.append(dv.Value.Value.axis)
        
        # If no starttime is given, results of read_raw_history are reversed.
        if starttime is None:
            xvalues.reverse()
            yvalues.reverse()
            dates.reverse()
            axes.reverse()
        return (xvalues, yvalues, dates, axes)

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

    starttime = pytz.utc.localize(
        datetime.datetime.strptime("2022-02-11 00:00:00", '%Y-%m-%d %H:%M:%S')
    )
    endtime = pytz.utc.localize(
        datetime.datetime.strptime("2022-03-24 00:00:00", '%Y-%m-%d %H:%M:%S')
    )

    # acquire history data
    (xvalues, yvalues, dates, axes) = await DataAcquisition.get_sensor_data(
        serverUrl=url,
        macId=macId,
        browseName="vibration",
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
    
    # plot data
    for i in range(len(dates)):
        plt.figure()
        plt.plot(xvalues[i],yvalues[i])
        date = axes[i] + " " + str(dates[i])
        plt.title(date)
        dateTitle = date.replace(" ", "_")
        dateTitle = dateTitle.replace(":","")
        figTitle = folder + '\image_' + dateTitle + '_' + str(i) + '.png'
        plt.savefig(figTitle)
        plt.close()

if __name__ == '__main__':
    asyncio.run(main())
