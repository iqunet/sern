import { request, GraphQLClient } from 'graphql-request'; // npm install --save graphql-request
import rq from 'request-promise-native'; // npm install --save request
                                         // npm install --save request-promise-native


function parseCookies(request){
    var list = {};
    var rc = request['headers']['set-cookie'][0];
    rc && rc.split(';').forEach(function(cookie){
        var parts = cookie.split('=');
        list[parts.shift().trim()] = decodeURI(parts.join('='));
    });
    return list;
}

async function createGraphQLClient(url){
    var host = url.split('//')[1].split('/')[0];
    var options = {
	uri: url,
	resolveWithFullResponse: true,
	headers: {
            'Host': host,
  	    'Accept': 'text/html',
	}
    };
    var response = await rq.get(options);
    var parsedCookie = parseCookies(response);
    var cookie = await rq.cookie('csrftoken=' + parsedCookie['csrftoken']);
  
    const client = new GraphQLClient(
        url,
	{
	    credentials: 'include',
	    headers: {
		'x-csrftoken': parsedCookie['csrftoken'],
		'Cookie': cookie,
	        'Content-Type' : 'application/json',
		'Accept' : 'application/json'
	    }
	}
    );
    return client;
}

async function main() {
    // create client (replace xx.xx.xx.xx with the IP address of your server)
    const url = 'http://xx.xx.xx.xx:8000/graphql';
    const queryClient = await createGraphQLClient(url);
	
    // construct query
    const query = `
    {
        deviceManager { 
            deviceList {
                parent macId tag
            } 
        }
    }
    `;
  
    // execute query and print device list
    const deviceList = await queryClient.request(query);
    console.log(deviceList.deviceManager.deviceList);  
}

main().catch(error => console.error(error))
