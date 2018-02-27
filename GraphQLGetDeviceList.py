# requires iQunet version > 1.2.2
# install gql from github

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests

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

    # construct query
    querytext = '''
    {
        deviceManager { 
            deviceList {
                parent macId tag
            } 
        }
    }
    '''
    
    # execute query and print device list
    result = executeQuery(client, querytext)
    print(result)
