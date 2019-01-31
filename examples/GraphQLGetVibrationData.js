import { request, GraphQLClient } from 'graphql-request';
import rq from 'request-promise-native';


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
    
    // construct query to retrieve vibration time stamps
    // replace macId xx:xx:xx:xx with the macId of your sensor
    // replace start and end with the desired start and end dates
    // limit limits the number of dates returned
    // axis allows to select data from only 1 or multiple axes
    const query = `
    { deviceManager { device(macId:"xx:xx:xx:xx") {
    __typename
    ... on GrapheneVibrationCombo {vibrationTimestampHistory(start:"2019-01-01T00:00:00.000000+00:00", end:"2019-02-01T00:00:00.000000+00:00", limit:100, axis:"X")}
    }}}
    `;
    
    // execute query and retrieve vibration time stamps
    var result = await queryClient.request(query);
    var timeStamps = result.deviceManager.device.vibrationTimestampHistory;
    console.log(timeStamps);
    
    // retrieve vibration data according to the vibration time stamps
    // replace macId xx:xx:xx:xx with the macId of your sensor
    var arrayLength = timeStamps.length;
    for (var i = 0; i < arrayLength; i++) {
        // construct query
        const query = `
        { deviceManager { device(macId:"xx:xx:xx:xx") {
        __typename
        ... on GrapheneVibrationCombo { vibrationArray(isoDate: "` + timeStamps[i] + `") {
        numSamples rawSamples sampleRate formatRange axis }}
        }}}
        `;
        
        // execute query
        var result = await queryClient.request(query);
        
        // retrieve raw acceleration data and format range
        var data = result.deviceManager.device.vibrationArray.rawSamples;
        var frange = result.deviceManager.device.vibrationArray.formatRange;
        
        // convert raw acceleration data to 'g' units
        var dataLength = data.length;
        for (var k = 0; k < dataLength; k++) {
            data[k] = data[k]/512.0*frange
        }
        console.log(data);
    }
}

main().catch(error => console.error(error))
