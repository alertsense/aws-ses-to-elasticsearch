const esName = process.env.ENDPOINT;
const prefix = process.env.PREFIX;
const region = process.env.REGION;

var AWS = require('aws-sdk');
var path = require('path');

/* == Globals == */
var esDomain = {
  region: region,
  endpoint: esName,
  index: `${prefix}-${getDate()}`,
  doctype: 'emails'
};
var endpoint = new AWS.Endpoint(esDomain.endpoint);
var creds = new AWS.EnvironmentCredentials('AWS');


exports.handler = function (event, context, callback) {

  const payload = event.Records[0].Sns;
  payload.Message = JSON.parse(payload.Message);
  const id = payload.MessageId;
  const requestPath = path.join('/', esDomain.index, esDomain.doctype, id, '_create');

  var req = new AWS.HttpRequest(endpoint);

  console.log('sending request to:', requestPath);

  const docRequest = JSON.stringify(payload);

  req.method = 'POST';
  req.path = requestPath;
  req.region = esDomain.region;
  req.headers['presigned-expires'] = false;
  req.headers['Host'] = endpoint.host;
  req.body = docRequest;

  var signer = new AWS.Signers.V4(req, 'es');  // es: service code
  signer.addAuthorization(creds, new Date());

  var send = new AWS.NodeHttpClient();
  send.handleRequest(req, null, function (httpResp) {
    var respBody = '';
    httpResp.on('data', function (chunk) {
      respBody += chunk;
    });
    httpResp.on('end', function (chunk) {
      console.log('Response: ' + respBody);
      context.succeed(`Published SNS Message to ElasticSearch ${id}`);
    });
  }, function (err) {
    console.log('Error: ' + err);
    context.fail('Lambda failed with error ' + err);
  });
};
function getDate() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), 1).toISOString().substring(0, 10);
}
