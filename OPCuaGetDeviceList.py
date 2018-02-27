from opcua import ua
from opcua import Client

def connectClient(url):
    client = Client(url)
    client.connect()
    return client

def disconnectClient(client):
    client.disconnect()
        
if __name__ == '__main__':
    
    # connect client (replace xx.xx.xx.xx with the IP address of your server)
    client = connectClient("opc.tcp://xx.xx.xx.xx:4840/freeopcua/server/")
    
    # get device list and print result
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    path = [ua.QualifiedName(name = 'Objects', namespaceidx = 0)]
    uaNode = client.get_root_node().get_child(path)
    nodes = uaNode.get_children()
    deviceList = []
    for n in nodes:
        name = n.get_display_name().Text
        if name != 'Server':
            deviceList.append(name)
    print(deviceList)
    
    # disconnect client
    disconnectClient(client)
