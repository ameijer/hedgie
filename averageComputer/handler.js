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
var AWS = require("aws-sdk");

var docClient = new AWS.DynamoDB.DocumentClient();

// the maximum period in hours to compute
const MAX_PERIOD = 60 * 24 * 14;

// this module computes hourly averages over preceeding periods
// used to update bot triggers
module.exports.computeAverages = (event, context, callback) => {

  //scan table from most recent up, updating averages every 12 hrs
  var scanResults = computeAverages(callback);
};

// helper function to compute back averages
function computeAverages(callback) {

  var now = (new Date).getTime();
  var maxAge = now - (60 * 24 * 60 * 60 * 1000);

  const params = {
    TableName: "PriceUpdates",
    ProjectionExpression: "#ts, price",
    KeyConditionExpression: "#typ = :type AND #ts BETWEEN :then AND :now",
    ScanIndexForward: false, // true = ascending, false = descending
    ExpressionAttributeNames: {
      "#ts": "timestamp",
      "#typ": "type"
    },
    ExpressionAttributeValues: {
      ":type": "BTC/USD",
      ":then": maxAge,
      ":now": now
    }
  };

  // scan price table, retrieve prices up to max age
  docClient.query(params, function(err, data) {
    if (err) {
      console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      console.log("Query succeeded.");
      var prices = new Array();
      data.Items.forEach(function(item) {
        item.timestamp = new Date(item.timestamp).toISOString();
        prices.push(item);
      });

      var arr = new Object();
      var period = 60;
      var result = 0.0;
      while (result != null) {
        if (period > prices.length || period >= MAX_PERIOD) {
          result = null;
        } else {
          //  compute average for period
          arr[(period / 60)] = calculate(period, prices);
          period = period + 60;
        }
      }

      // persist the computed average to the table
      saveAverages(arr).then(function() {
        const response = {
          statusCode: 200,
          body: JSON.stringify({
            message: 'sucessfully updated averages',
            output: JSON.stringify(arr)
          }),
        };

        callback(null, response);

      });
    }
  });
}


function calculate(period, prices) {
  var sum = 0.0;

  for (var i = 0; i < period; i++) {
    sum = Number(sum) + Number(prices[i].price);
  }

  return (Number(sum) / Number(period));
}

// persist the averages to a dynamodb table
function saveAverages(d) {
  var saveParams = {
    TableName: "Averages",
    Item: {
      "type": "BTC/USD",
      "timestamp": (new Date).getTime(),
      "averages": d
    }
  };

  console.log("saving following: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}
