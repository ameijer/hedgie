/**
 * @Author: Alex Meijer <alex>
 * @Date:   05-Aug-2018
 * @Project: hedgie
 * @Filename: handler.js
 * @Last modified by:   alex
 * @Last modified time: 05-Mar-2019
 * @License: See LICENSE file for license terms
 * @Copyright: Copyright 2018 Alex Meijer. All Rights Reserved
 */

'use strict';

var https = require('https');
var AWS = require("aws-sdk");
AWS.config.update({
  region: "us-east-1"
});

var docClient = new AWS.DynamoDB.DocumentClient();

const tableName = "PriceUpdates";

//options used to query the btc exchange api server for price
const options = {
  host: 'api.gemini.com',
  port: 443,
  path: '/v1/pubticker/btcusd',
  method: 'GET'
};

// serverless entry point. Indended to be configured as a simple polling device,
// which saves prices from a bitcoin exchange to a Dynamo table
module.exports.handler = (event, context, callback) => {

  console.log('event: ' + JSON.stringify(event));
  console.log('context: ' + JSON.stringify(context));
  https.request(options, function(res) {
    res.setEncoding('utf8');
    res.on('data', function(d) {
      console.log('raw data from http call: ' + d);
      process(d, callback);
    });
  }).end();
};

// helper function to save price trend in db
function process(d, callback) {

  var evald = JSON.parse(d);

  // when the response is malformed (exchange-side error, etc)
  // terminate the function immediately
  if (typeof evald.last == "undefined") {
    console.log("no last price in object, so not saving");
    callback(null, {
      "result": "nothing saved"
    });
    return;
  }

  savePrice(evald).then(function() {
    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: 'saved data: ' + d
      })
    };

    callback(null, response);
  });
}

// persist price object to Dynamo
function savePrice(d) {
  var saveParams = {
    TableName: "PriceUpdates",
    Item: {
      "type": "BTC/USD",
      "timestamp": (new Date).getTime(),
      "price": d.last,
      "exchange": options,
      "detailed": d

    }
  };

  console.log('saving price with following dynamodb parameter: ' + JSON.stringify(saveParams));

  // promisify return value
  return docClient.put(saveParams).promise();
}
