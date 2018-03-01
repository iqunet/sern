from opcua import ua
from opcua import Client
        
if __name__ == '__main__':
    
    # create client (replace xx.xx.xx.xx with the IP address of your server)
    client = Client("opc.tcp://xx.xx.xx.xx:4840/freeopcua/server/")
    
    try:
        # connect client
        client.connect()        
        
        # get device list and print result
        path = [ua.QualifiedName(name = 'Objects', namespaceidx = 0)]
        uaNode = client.get_root_node().get_child(path)
        nodes = uaNode.get_children()
        deviceList = []
        for n in nodes:
            name = n.get_display_name().Text
            if name != 'Server':
                deviceList.append(name)
        print(deviceList)
    
    finally:
        # disconnect client
        client.disconnect()
