import time
import sys
import pytz
import logging
import datetime
from urllib.parse import urlparse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    MAX_VALUES_PER_ENDNODE = 10000  # Num values per endnode
    MAX_VALUES_PER_REQUEST = 10  # Num values per history request
   
    @staticmethod
    def get_sensor_sub_node(client, macId, browseName, subBrowseName, sub2BrowseName=None, sub3BrowseName=None, sub4BrowseName=None):
        nsIdx = client.get_namespace_index(
                'http://www.iqunet.com'
        )  # iQunet namespace index
        bpath = [
                ua.QualifiedName(name=macId, namespaceidx=nsIdx),
                ua.QualifiedName(name=browseName, namespaceidx=nsIdx),
                ua.QualifiedName(name=subBrowseName, namespaceidx=nsIdx)
        ]
        if sub2BrowseName is not None:
            bpath.append(ua.QualifiedName(name=sub2BrowseName, namespaceidx=nsIdx))
        if sub3BrowseName is not None:
            bpath.append(ua.QualifiedName(name=sub3BrowseName, namespaceidx=nsIdx))
        if sub4BrowseName is not None:
            bpath.append(ua.QualifiedName(name=sub4BrowseName, namespaceidx=nsIdx))
        sensorNode = client.objectsNode.get_child(bpath)
        return sensorNode
    
    @staticmethod
    def get_endnode_data(client, endNode, starttime, endtime):
        dvList = DataAcquisition.download_endnode(
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

    @staticmethod
    def get_anomaly_model_nodes(client, macId):
        sensorNode = \
            DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models")
        DataAcquisition.LOGGER.info(
                'Browsing for models of {:s}'.format(macId)
        )
        modelNodes = sensorNode.get_children()
        return modelNodes
    
    @staticmethod
    def get_anomaly_model_parameters(client, macId, starttime, endtime):
        modelNodes = \
            DataAcquisition.get_anomaly_model_nodes(client, macId)
        models = dict()
        for mnode in modelNodes:
            key = mnode.get_display_name().Text
            sensorNode = \
                 DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE")
            (valuesraw, datesraw) = \
                DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_05pct")
            (values05, dates05) = \
                DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_50pct")
            (values50, dates50) = \
                DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                  DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "expectile_95pct")
            (values95, dates95) = \
                DataAcquisition.get_endnode_data(
                    client=client,
                    endNode=sensorNode,
                    starttime=starttime,
                    endtime=endtime
                )
            sensorNode = \
                 DataAcquisition.get_sensor_sub_node(client, macId, "tensorFlow", "models", key, "lossMAE", "alarmLevel")
            alarmLevel = sensorNode.get_value()
            modelSet = {
                "raw": (valuesraw, datesraw),
                "expectile_05pct": (values05, dates05),
                "expectile_50pct": (values50, dates50),
                "expectile_95pct": (values95, dates95),
                "alarmLevel": alarmLevel
                }
            models[key] = modelSet
        return models

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("opcua").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    serverIP = "xx.xx.xx.xx"
    serverUrl = urlparse('opc.tcp://{:s}:4840'.format(serverIP))

    # replace xx:xx:xx:xx with your sensors macId
    macId = 'xx:xx:xx:xx'
    
    # change settings
    hpf = 3 # high pass filter (Hz)
    startTime = "2019-02-15 00:00:00"
    endTime = "2021-02-15 10:00:00"
    timeZone = "Europe/Brussels" # local time zone
    
    # format start and end time
    starttime = pytz.utc.localize(
        datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
    )
    endtime = pytz.utc.localize(
        datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S')
    )
    
    # create opc ua client
    with OpcUaClient(serverUrl) as client:
        assert(client._client.uaclient._uasocket.timeout == 15)
    
        # acquire model data
        modelDict = DataAcquisition.get_anomaly_model_parameters(
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
