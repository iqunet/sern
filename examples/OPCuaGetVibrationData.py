import pytz
import datetime
from opcua import ua
from opcua import Client

import matplotlib.pyplot as plt
    

def readHistory(macId, browseName = None, startTime = None, endTime = None, numValues = 8192):
    
    # get variable handle
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = browseName, namespaceidx = nsIdx))  
    uaNode = client.get_root_node().get_child(bpath)
    
    # get history data of node
    if startTime and endTime and startTime > endTime:
        return
    historyData = uaNode.read_raw_history(starttime = startTime, endtime = endTime, numvalues = numValues)
    values = []
    dates = []
    for sample in historyData:
        dates.append(pytz.utc.localize(sample.SourceTimestamp))
        values.append(sample.Value.Value)
    # if no starttime is given, results of read_raw_history are reversed.
    if startTime is None:
        values.reverse()
        dates.reverse()
    return (values, dates)
    
if __name__ == '__main__':
    
    # create client (replace xx.xx.xx.xx with the IP address of your server)
    client = Client("opc.tcp://xx.xx.xx.xx:4840/freeopcua/server/")
    
    try:
        # connect client
        client.connect()        
        
        # choose start and end dates
        startDate = pytz.utc.localize(datetime.datetime.strptime("2018-02-23 00:00:00", '%Y-%m-%d %H:%M:%S'))        
        endDate = pytz.utc.localize(datetime.datetime.strptime("2018-02-24 00:00:00", '%Y-%m-%d %H:%M:%S'))
        
        # retrieve vibration data (replace xx:xx:xx:xx with the macId of your sensor)
        (values,dates) = readHistory("xx:xx:xx:xx", browseName = "accelerationPack", startTime = startDate, endTime = endDate)
       
        # convert vibration data to 'g' units and plot data
        data = [val[1:-6] for val in values]
        formatRanges = [val[-5] for val in values]
        for i in range(len(formatRanges)):
            data[i] = [d/512.0*formatRanges[i] for d in data[i]]
            plt.figure()
            plt.plot(data[i])
            plt.title(str(dates[i]))
                
    finally:
        # disconnect client
        client.disconnect()
