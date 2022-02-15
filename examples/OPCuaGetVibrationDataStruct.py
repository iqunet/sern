
import time
import sys
import pytz
import logging
import datetime
import itertools
from urllib.parse import urlparse
import matplotlib.pyplot as plt

from opcua import ua, Client


class OpcUaClient(object):
    CONNECT_TIMEOUT = 15  # [sec]
    RETRY_DELAY = 10  # [sec]
    MAX_RETRIES = 3  # [-]

    class Decorators(object):
        @staticmethod
        def autoConnectingClient(wrappedMethod):
            def wrapper(obj, *args, **kwargs):
                for retry in range(OpcUaClient.MAX_RETRIES):
                    try:
                        return wrappedMethod(obj, *args, **kwargs)
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
                        time.sleep(OpcUaClient.RETRY_DELAY)
                else:  # So the exception is exposed.
                    obj.reconnect()
                    return wrappedMethod(obj, *args, **kwargs)
            return wrapper

    def __init__(self, serverUrl):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = Client(
            serverUrl.geturl(),
            timeout=self.CONNECT_TIMEOUT
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()
        self._client = None

    @property
    @Decorators.autoConnectingClient
    def sensorList(self):
        return self.objectsNode.get_children()

    @property
    @Decorators.autoConnectingClient
    def objectsNode(self):
        path = [ua.QualifiedName(name='Objects', namespaceidx=0)]
        return self._client.get_root_node().get_child(path)

    def connect(self):
        self._client.connect()
        self._client.load_type_definitions()

    def disconnect(self):
        try:
            self._client.disconnect()
        except Exception:
            pass

    def reconnect(self):
        self.disconnect()
        self.connect()

    @Decorators.autoConnectingClient
    def get_browse_name(self, uaNode):
        return uaNode.get_browse_name()

    @Decorators.autoConnectingClient
    def get_node_class(self, uaNode):
        return uaNode.get_node_class()

    @Decorators.autoConnectingClient
    def get_namespace_index(self, uri):
        return self._client.get_namespace_index(uri)

    @Decorators.autoConnectingClient
    def get_child(self, uaNode, path):
        return uaNode.get_child(path)

    @Decorators.autoConnectingClient
    def read_raw_history(self,
                         uaNode,
                         starttime=None,
                         endtime=None,
                         numvalues=0,
                         cont=None):
        details = ua.ReadRawModifiedDetails()
        details.IsReadModified = False
        details.StartTime = starttime or ua.get_win_epoch()
        details.EndTime = endtime or ua.get_win_epoch()
        details.NumValuesPerNode = numvalues
        details.ReturnBounds = True
        result = OpcUaClient._history_read(uaNode, details, cont)
        assert(result.StatusCode.is_good())
        return result.HistoryData.DataValues, result.ContinuationPoint

    @staticmethod
    def _history_read(uaNode, details, cont):
        valueid = ua.HistoryReadValueId()
        valueid.NodeId = uaNode.nodeid
        valueid.IndexRange = ''
        valueid.ContinuationPoint = cont

        params = ua.HistoryReadParameters()
        params.HistoryReadDetails = details
        params.TimestampsToReturn = ua.TimestampsToReturn.Both
        params.ReleaseContinuationPoints = False
        params.NodesToRead.append(valueid)
        result = uaNode.server.history_read(params)[0]
        return result


class DataAcquisition(object):
    LOGGER = logging.getLogger('DataAcquisition')
    AXES = ('x', 'y', 'z')
    ORDINATES = ('accel', 'veloc')
    DOMAINS = ('time', 'freq')
    MAX_VALUES_PER_ENDNODE = 100  # Num values per endnode
    MAX_VALUES_PER_REQUEST = 2  # Num values per history request

    @staticmethod
    def selected_to_workbook(serverUrl,
                             macIdsToCollect,
                             starttime,
                             endtime):
        with OpcUaClient(serverUrl) as client:
            for sensorNode in client.sensorList:
                assert(client._client.uaclient._uasocket.timeout == 15)
                macId = client.get_browse_name(sensorNode).Name
                if macId not in macIdsToCollect:
                    DataAcquisition.LOGGER.info(
                        'Skipping sensor {:s}'.format(macId)
                    )
                    continue
                tagPath = ua.QualifiedName(
                    'deviceTag',
                    sensorNode.nodeid.NamespaceIndex
                )
                DataAcquisition.LOGGER.info(
                    'Processing sensor {:s} ({:s})'.format(
                        macId,
                        client.get_child(sensorNode, tagPath).get_value()
                    )
                )
                DataAcquisition.get_sensor_data(
                        client,
                        sensorNode,
                        starttime,
                        endtime
                )

    @staticmethod
    def get_sensor_data(serverUrl, macId, browseName, starttime, endtime):
        allValues = []
        allDates = []
        with OpcUaClient(serverUrl) as client:
            assert(client._client.uaclient._uasocket.timeout == 15)
            sensorNode = DataAcquisition.get_sensor_node(
                    client,
                    macId,
                    browseName
            )
            for path in DataAcquisition.endnodes_path_generator(sensorNode):
                DataAcquisition.LOGGER.info(
                        'Browsing {:s} -> {:s}'.format(
                                macId,
                                sensorNode.get_browse_name().Name
                            )
                )
                endNode = client.get_child(sensorNode, path)
                (values, dates) = DataAcquisition.get_endnode_data(
                        client,
                        endNode,
                        starttime,
                        endtime
                )
                allValues.extend(values)
                allDates.extend(dates)
        return (allValues, allDates)

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
    def get_sensor_node(client, macId, browseName):
        nsIdx = client.get_namespace_index(
                'http://www.iqunet.com'
        )  # iQunet namespace index
        bpath = [
                ua.QualifiedName(name=macId, namespaceidx=nsIdx),
                ua.QualifiedName(name=browseName, namespaceidx=nsIdx)
                ]
        sensorNode = client.objectsNode.get_child(bpath)
        return sensorNode

    @staticmethod
    def get_endnode_data(client, endNode, starttime, endtime):
        dvList = DataAcquisition.download_endnode(
                client,
                endNode,
                starttime,
                endtime
        )
        dates, values = ([], [])
        for dv in dvList:
            dates.append(dv.SourceTimestamp.strftime('%Y-%m-%d %H:%M:%S'))
            values.append(dv.Value.Value.y_ordinate)

        # If no starttime is given, results of read_raw_history are reversed.
        if starttime is None:
            values.reverse()
            dates.reverse()
        return (values, dates)

    @staticmethod
    def download_endnode(client, endNode, starttime, endtime):
        endNodeName = client.get_browse_name(endNode).Name
        DataAcquisition.LOGGER.info(
                'Downloading endnode {:s}'.format(
                        endNodeName
                    )
        )
        dvList, contId = [], None
        while True:
            remaining = DataAcquisition.MAX_VALUES_PER_ENDNODE - len(dvList)
            assert(remaining >= 0)
            numvalues = min(DataAcquisition.MAX_VALUES_PER_REQUEST, remaining)
            partial, contId = client.read_raw_history(
                uaNode=endNode,
                starttime=starttime,
                endtime=endtime,
                numvalues=numvalues,
                cont=contId
            )
            if not len(partial):
                DataAcquisition.LOGGER.warning(
                    'No data was returned for {:s}'.format(endNodeName)
                )
                break
            dvList.extend(partial)
            sys.stdout.write('\r    Loaded {:d} values, {:s} -> {:s}'.format(
                len(dvList),
                str(dvList[0].ServerTimestamp.strftime("%Y-%m-%d %H:%M:%S")),
                str(dvList[-1].ServerTimestamp.strftime("%Y-%m-%d %H:%M:%S"))
            ))
            sys.stdout.flush()
            if contId is None:
                break  # No more data.
            if len(dvList) >= DataAcquisition.MAX_VALUES_PER_ENDNODE:
                break  # Too much data.
        sys.stdout.write('...OK.\n')
        return dvList


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("opcua").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    serverIP = "xx.xx.xx.xx"
    serverUrl = urlparse('opc.tcp://{:s}:4840'.format(serverIP))

    # replace xx:xx:xx:xx with your sensors macId
    macId = 'xx:xx:xx:xx'

    starttime = pytz.utc.localize(
        datetime.datetime.strptime("2020-02-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    )
    endtime = pytz.utc.localize(
        datetime.datetime.strptime("2020-02-24 00:00:00", '%Y-%m-%d %H:%M:%S')
    )

    # acquire history data
    (values, dates) = DataAcquisition.get_sensor_data(
        serverUrl=serverUrl,
        macId=macId,
        browseName="vibration",
        starttime=starttime,
        endtime=endtime
    )

    # plot data
    for i in range(len(dates)):
        plt.figure()
        plt.plot(values[i])
        plt.title(str(dates[i]))
