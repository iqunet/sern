import logging
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
    
    @staticmethod
    async def get_device_list(serverUrl):
        deviceList = dict()
        async with OpcUaClient(serverUrl) as client:
            for sensorNode in await client.get_sensor_list():
                macId = await client.read_browse_name(sensorNode)
                macId = macId.Name
                if (await client.read_node_class(sensorNode) is ua.NodeClass.Object) \
                        and ("server" not in macId.lower()):
                    try:
                        tagPath = ua.QualifiedName(
                            'deviceTag',
                            sensorNode.nodeid.NamespaceIndex
                        )
                        ch = await client.get_child(sensorNode, tagPath)
                        deviceTag = await ch.get_value()
                        if deviceTag != 'delete':
                            deviceList[macId] = '{:s}'.format(deviceTag)
                    except Exception:
                        deviceList[macId] = 'Device'
                        continue
        return deviceList

async def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("opcua").setLevel(logging.WARNING)

    # replace xx.xx.xx.xx with the IP address of your server
    url: str = 'opc.tcp://xx.xx.xx.xx:4840/freeopcua/server'
    
    deviceList = await DataAcquisition.get_device_list(serverUrl=url)
    print(deviceList)

if __name__ == '__main__':
    asyncio.run(main())
