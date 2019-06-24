from opcua import ua
from opcua import Client
        
if __name__ == '__main__':
    
    # create client (replace xx.xx.xx.xx with the IP address of your server)
    client = Client("opc.tcp://xx.xx.xx.xx:4840/freeopcua/server/")
    
    try:
        # connect client
        client.connect()        
        
        # get device list and print result
        nsIdx = client.get_namespace_index('http://www.iqunet.com')
        path = [ua.QualifiedName(name = 'Objects', namespaceidx = 0)]
        uaNode = client.get_root_node().get_child(path)
        nodes = uaNode.get_children()
        deviceList = dict()
        for n in nodes:
            macId = n.get_display_name().Text
            
            if macId != 'Server':
                bpath = []
                bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
                bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
                bpath.append(ua.QualifiedName(name = "deviceTag", namespaceidx = nsIdx))  
                uaNode2 = client.get_root_node().get_child(bpath)
                deviceTag = uaNode2.get_value()
                deviceList[macId] = deviceTag
        print(deviceList)
    
    finally:
        # disconnect client
        client.disconnect()
