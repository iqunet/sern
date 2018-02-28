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
    
    # read history from variable
    dates, values, starttime, endtime = ([], [], startTime, endTime)
    # In each iteration, 1024 samples are extracted.
    while numValues > 0:
        if starttime and endtime and starttime > endtime:
            break
        historyReadResult = readRawHistory(
            uaClient = client.uaclient,
            uaNodeId = uaNode.nodeid,
            starttime = starttime,
            endtime = endtime,
            numvalues = min(numValues, 1024)
        )
        dvArr = historyReadResult.HistoryData.DataValues
        if len(dvArr) == 0:
            break
        numValues -= len(dvArr)
        for sample in dvArr:
            dates.append(pytz.utc.localize(sample.SourceTimestamp))
            values.append(sample.Value.Value)
        # Set new endtime to 'oldest time stamp' minus 1us. For opcua history, timestamps
        # used in read_raw_history(...) are ServerTimestamp, not SourceTimestamp!
        if starttime is None:
            # read_raw_history returns [oldest..newest]. Continue reading at 'newest - 1us'
            endtime = pytz.utc.localize(dvArr[-1].ServerTimestamp) - datetime.timedelta(microseconds=1)
        else:
            # read_raw_history returns [newest..oldest]. Continue reading at 'oldest + 1us'
            starttime = pytz.utc.localize(dvArr[-1].ServerTimestamp) + datetime.timedelta(microseconds=1)
    # If no starttime is given, results of read_raw_history are reversed.
    if starttime is None:
        values.reverse()
        dates.reverse()
    return (values, dates) 

def readRawHistory(uaClient = None, uaNodeId = None, starttime = None, endtime = None, numvalues = 0):
    details = ua.ReadRawModifiedDetails()
    details.IsReadModified = False
    details.StartTime = starttime if starttime else ua.get_win_epoch()
    details.EndTime = endtime if endtime else ua.get_win_epoch()
    details.NumValuesPerNode = numvalues
    details.ReturnBounds = True
    
    valueid = ua.HistoryReadValueId()
    valueid.NodeId = uaNodeId
    valueid.IndexRange = ''

    params = ua.HistoryReadParameters()
    params.HistoryReadDetails = details
    params.TimestampsToReturn = ua.TimestampsToReturn.Both
    params.ReleaseContinuationPoints = False
    params.NodesToRead.append(valueid)
    
    historyReadResult = uaClient.history_read(params)[0]
    
    return historyReadResult

def readHistory2(macId, browseName = None, startTime = None, endTime = None, numValues = 8192):
    
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
    
def readHistory3(macId, browseName = None, startTime = None, endTime = None, numValues = 8192):
    # get variable handle
    nsIdx = client.get_namespace_index('http://www.iqunet.com')
    bpath = []
    bpath.append(ua.QualifiedName(name = 'Objects', namespaceidx = 0))
    bpath.append(ua.QualifiedName(name = macId, namespaceidx = nsIdx))
    bpath.append(ua.QualifiedName(name = browseName, namespaceidx = nsIdx))  
    uaNode = client.get_root_node().get_child(bpath)
        
    # read history from variable
    dates, values, starttime, endtime = ([], [], startTime, endTime)
    # In each iteration, 1024 samples are extracted.
    while numValues > 0:
        if starttime and endtime and starttime > endtime:
            break
        historyData = uaNode.read_raw_history(starttime = starttime, endtime = endtime, numvalues = min(numValues, 1024))
        dvArr = historyData
        if len(dvArr) == 0:
            break
        numValues -= len(dvArr)
        for sample in dvArr:
            dates.append(pytz.utc.localize(sample.SourceTimestamp))
            values.append(sample.Value.Value)
        # Set new endtime to 'oldest time stamp' minus 1us. For opcua history, timestamps
        # used in read_raw_history(...) are ServerTimestamp, not SourceTimestamp!
        if starttime is None:
            # read_raw_history returns [oldest..newest]. Continue reading at 'newest - 1us'
            endtime = pytz.utc.localize(dvArr[-1].ServerTimestamp) - datetime.timedelta(microseconds=1)
        else:
            # read_raw_history returns [newest..oldest]. Continue reading at 'oldest + 1us'
            starttime = pytz.utc.localize(dvArr[-1].ServerTimestamp) + datetime.timedelta(microseconds=1)
    # If no starttime is given, results of read_raw_history are reversed.
    if starttime is None:
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
        
        # retrieve vibration data (replace xx:xx:xx:xx with the macId of your sensor)
        (values, dates) = readHistory2("xx:xx:xx:xx", browseName = "accelerationPack", startTime = startDate, endTime = endDate)
        
        # retrieve vibration data (replace xx:xx:xx:xx with the macId of your sensor)
        (values,dates) = readHistory3("xx:xx:xx:xx", browseName = "accelerationPack", startTime = startDate, endTime = endDate)
       
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
