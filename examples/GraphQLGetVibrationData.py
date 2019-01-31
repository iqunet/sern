# requires iQunet version > 1.2.2
# install gql from github (pip install -e git+git://github.com/graphql-python/gql.git#egg=gql)

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests

import matplotlib.pyplot as plt

def createClient(url):
    host = url.split('//')[1].split('/')[0]
    request = requests.get(url,
                           headers={
                               'Host': str(host),
                               'Accept': 'text/html',
                           })
    request.raise_for_status()
    csrf = request.cookies['csrftoken']

    client = Client(
        transport=RequestsHTTPTransport(url=url,
                                        cookies={"csrftoken": csrf},
                                        headers={'x-csrftoken':  csrf}),
        fetch_schema_from_transport=True
    ) 
    return client
    
def executeQuery(client, querytext):
    query = gql(querytext)
    result = client.execute(query)
    return result

if __name__ == '__main__':
    
    # create client (replace xx.xx.xx.xx with the IP address of your server)
    client = createClient('http://xx.xx.xx.xx:8000/graphql')
    
    # construct query to retrieve vibration time stamps
    # replace macId xx:xx:xx:xx with the macId of your sensor
    # replace start and end with the desired start and end dates
    # limit limits the number of dates returned
    # axis allows to select data from only 1 or multiple axes
    querytext = '''
    { deviceManager { device(macId:"xx:xx:xx:xx") {
    __typename
    ... on GrapheneVibrationCombo {vibrationTimestampHistory(start:"2019-01-01T00:00:00.000000+00:00", end:"2019-02-01T00:00:00.000000+00:00", limit:100, axis:"X")}
    }}}
    '''
    
    # execute query and retrieve vibration time stamps
    result = executeQuery(client, querytext)
    times = result['deviceManager']['device']['vibrationTimestampHistory']
    print(times)
    
    # retrieve vibration data according to the vibration time stamps and plot data
    # replace macId xx:xx:xx:xx with the macId of your sensor
    for t in times:
        # construct query
        querytext = '''
        { deviceManager { device(macId:"xx:xx:xx:xx") {
        __typename
        ... on GrapheneVibrationCombo { vibrationArray(isoDate: "''' + t + '''") {
        numSamples rawSamples sampleRate formatRange axis }}
        }}}
        '''
        # execute query
        result = executeQuery(client, querytext)
        # retrieve raw acceleration data and format range
        data = result['deviceManager']['device']['vibrationArray']['rawSamples']
        frange = result['deviceManager']['device']['vibrationArray']['formatRange']
        # convert raw acceleration data to 'g' units
        data = [d/512.0*frange for d in data]
        # plot data
        plt.figure()
        plt.plot(data)
        plt.title(str(t))
