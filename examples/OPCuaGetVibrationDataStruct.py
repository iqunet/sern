from opcua import ua
from opcua import Client
import matplotlib.pyplot as plt

def getFrequencyDomainAccelerationData(client, macId, axis):
    
    # axis to lower case
    axis = axis.lower()
    
    # get node
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = "vibration", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis, namespaceidx = nsIdx)) 
    bpath.append(ua.QualifiedName(name = "accel", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis + "AccelFreq", namespaceidx = nsIdx)) 
    uaNode = client.get_root_node().get_child(bpath)
    
    # load vibration data
    client.load_type_definitions()  # scan server for custom structures and import them
    vibrationData = uaNode.get_data_value()
    
    # create dictionary
    vibDict = dict()
    vibrationStruct = vibrationData.Value.Value
    vibDict['SourceTimestamp'] = vibrationData.SourceTimestamp
    vibDict['ServerTimestamp'] = vibrationData.ServerTimestamp
    vibDict['y_ordinate'] = vibrationStruct.y_ordinate
    vibDict['x_abscissa'] = vibrationStruct.x_abscissa
    vibDict['sampleRate'] = vibrationStruct.sampleRate
    vibDict['formatRange'] = vibrationStruct.formatRange
    vibDict['axis'] = vibrationStruct.axis
    vibDict['vUnits'] = vibrationStruct.vUnits
    vibDict['freqDomain'] = vibrationStruct.freqDomain
            
    return vibDict
    
def getTimeDomainAccelerationData(client, macId, axis):
    
    # axis to lower case
    axis = axis.lower()
    
    # get node
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = "vibration", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis, namespaceidx = nsIdx)) 
    bpath.append(ua.QualifiedName(name = "accel", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis + "AccelTime", namespaceidx = nsIdx)) 
    uaNode = client.get_root_node().get_child(bpath)
    
    # load vibration data
    client.load_type_definitions()  # scan server for custom structures and import them
    vibrationData = uaNode.get_data_value()
    
    # create dictionary
    vibDict = dict()
    vibrationStruct = vibrationData.Value.Value
    vibDict['SourceTimestamp'] = vibrationData.SourceTimestamp
    vibDict['ServerTimestamp'] = vibrationData.ServerTimestamp
    vibDict['y_ordinate'] = vibrationStruct.y_ordinate
    vibDict['x_abscissa'] = vibrationStruct.x_abscissa
    vibDict['sampleRate'] = vibrationStruct.sampleRate
    vibDict['formatRange'] = vibrationStruct.formatRange
    vibDict['axis'] = vibrationStruct.axis
    vibDict['vUnits'] = vibrationStruct.vUnits
    vibDict['freqDomain'] = vibrationStruct.freqDomain
            
    return vibDict
    
def getFrequencyDomainVelocityData(client, macId, axis):
    
    # axis to lower case
    axis = axis.lower()
    
    # get node
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = "vibration", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis, namespaceidx = nsIdx)) 
    bpath.append(ua.QualifiedName(name = "veloc", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis + "VelocFreq", namespaceidx = nsIdx)) 
    uaNode = client.get_root_node().get_child(bpath)
    
    # load vibration data
    client.load_type_definitions()  # scan server for custom structures and import them
    vibrationData = uaNode.get_data_value()
    
    # create dictionary
    vibDict = dict()
    vibrationStruct = vibrationData.Value.Value
    vibDict['SourceTimestamp'] = vibrationData.SourceTimestamp
    vibDict['ServerTimestamp'] = vibrationData.ServerTimestamp
    vibDict['y_ordinate'] = vibrationStruct.y_ordinate
    vibDict['x_abscissa'] = vibrationStruct.x_abscissa
    vibDict['sampleRate'] = vibrationStruct.sampleRate
    vibDict['formatRange'] = vibrationStruct.formatRange
    vibDict['axis'] = vibrationStruct.axis
    vibDict['vUnits'] = vibrationStruct.vUnits
    vibDict['freqDomain'] = vibrationStruct.freqDomain
            
    return vibDict
    
def getTimeDomainVelocityData(client, macId, axis):
    
    # axis to lower case
    axis = axis.lower()
    
    # get node
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = "vibration", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis, namespaceidx = nsIdx)) 
    bpath.append(ua.QualifiedName(name = "veloc", namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = axis + "VelocTime", namespaceidx = nsIdx)) 
    uaNode = client.get_root_node().get_child(bpath)
    
    # load vibration data
    client.load_type_definitions()  # scan server for custom structures and import them
    vibrationData = uaNode.get_data_value()
    
    # create dictionary
    vibDict = dict()
    vibrationStruct = vibrationData.Value.Value
    vibDict['SourceTimestamp'] = vibrationData.SourceTimestamp
    vibDict['ServerTimestamp'] = vibrationData.ServerTimestamp
    vibDict['y_ordinate'] = vibrationStruct.y_ordinate
    vibDict['x_abscissa'] = vibrationStruct.x_abscissa
    vibDict['sampleRate'] = vibrationStruct.sampleRate
    vibDict['formatRange'] = vibrationStruct.formatRange
    vibDict['axis'] = vibrationStruct.axis
    vibDict['vUnits'] = vibrationStruct.vUnits
    vibDict['freqDomain'] = vibrationStruct.freqDomain
            
    return vibDict
    
if __name__ == '__main__':
    
    # create client (replace xx.xx.xx.xx with the IP address of your server)
    client = Client("opc.tcp://xx.xx.xx.xx:4840/freeopcua/server/")
    
    try:
        # connect client
        client.connect()        
        
        # retrieve vibration data (replace xx:xx:xx:xx with the macId of your sensor and select the x, y or z axis)
        freqAcc = getFrequencyDomainAccelerationData(client, "xx:xx:xx:xx", "x")
        timeAcc = getTimeDomainAccelerationData(client, "xx:xx:xx:xx", "x")
        freqVel = getFrequencyDomainVelocityData(client, "xx:xx:xx:xx", "x")
        timeVel = getTimeDomainVelocityData(client, "xx:xx:xx:xx", "x")
        
        # plot vibration data
        plt.figure()
        plt.plot(freqAcc['x_abscissa'],freqAcc['y_ordinate'])
        plt.title("AccelFreq")
        plt.figure()
        plt.plot(timeAcc['x_abscissa'],timeAcc['y_ordinate'])
        plt.title("AccelTime")
        plt.figure()
        plt.plot(freqVel['x_abscissa'],freqVel['y_ordinate'])
        plt.title("VelocFreq")
        plt.figure()
        plt.plot(timeVel['x_abscissa'],timeVel['y_ordinate'])
        plt.title("VelocTime")    
                
    finally:
        # disconnect client
        client.disconnect()
