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

AWS.config.update({
  region: "us-east-1"
});

var docClient = new AWS.DynamoDB.DocumentClient()

// function attempting to use DynamoDB atomic operations to modify a table
function updateTradeCounts(accountId, type, isHedge, executedAmt, price) {

  // start with a basic dynamodb string and append to it
  var updateString = 'ADD totalTrades :val1, ';
  var delta = Number(executedAmt);
  var deltaUSD = Number(executedAmt) * Number(price);
  if (type === 'sell') {
    updateString += 'sells :val1, exchangeBalanceBTC :delta, exchangeBalanceUSD :deltaUSD';
    delta *= -1;
  } else {
    updateString += 'buys :val1, exchangeBalanceBTC :delta, exchangeBalanceUSD :deltaUSD';
    deltaUSD *= -1;
  }

  if (isHedge) {
    updateString += ', hedges :val1';
  }

  updateString += ', volumeBTC :executedAmt';
  updateString += ', volumeUSD :executedAmtUSD';

  updateString += ' SET lastUpdated = :time';
  var params = {
    TableName: 'Metrics',
    Key: {
      "accountId": Number(accountId)
    },
    UpdateExpression: updateString,
    ExpressionAttributeValues: {
      ":val1": 1,
      ":executedAmt": Number(executedAmt),
      ":executedAmtUSD": (Number(executedAmt) * Number(price)),
      ":delta": delta,
      ":deltaUSD": deltaUSD,
      ":time": new Date().getTime()
    },
    ReturnValues: "UPDATED_NEW"
  };

  console.log("Updating the item with params: " + JSON.stringify(params));
  // atomic update call
  return docClient.update(params).promise();

}

// since we only run UPDATE calls here to leverage atomic ops,
// there must be an initialization of the base object
// that's what initializeObject does
function initializeObject(accountId, initialBalanceUSD) {

  var params = {
    TableName: "Metrics",
    ConditionExpression: "attribute_not_exists(accountId)",
    Item: {
      "accountId": Number(accountId)
    }
  };

  console.log("Saving the item with params: " + JSON.stringify(params));

  // dynamo atomic operations
  var paramsUpdate = {
    TableName: 'Metrics',
    Key: {
      "accountId": Number(accountId)
    },
    ConditionExpression: "attribute_not_exists(exchangeBalanceUSD)",
    UpdateExpression: "ADD exchangeBalanceUSD = :val1",
    ExpressionAttributeValues: {
      ":val1": initialBalanceUSD
    },
    ReturnValues: "UPDATED_NEW"
  };

  // put rather than update, as nothin exists prior
  docClient.put(params, function(err, data) {
    if (err) {
      console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      console.log('invoking callback with param: ' + JSON.stringify(data));
    }
    return docClient.update(paramsUpdate).promise();
  });

}

// entry point for serverless function
module.exports.metrics = (event, context, callback) => {

  console.log('event: ' + JSON.stringify(event));
  console.log('context: ' + JSON.stringify(context));

  var message = JSON.parse(event.Records[0].Sns.Message);

  // listen on the raw executed trades feed to provide a direct info source
  if (message.table === 'Trades') {
    console.log('Found message from Trades feed: ' + JSON.stringify(message) + ', acting on it');

    const type = message.dynamodb.NewImage.side["S"];
    var hedge = false;

    if (typeof(message.dynamodb.NewImage.hedge) !== 'undefined') {
      hedge = message.dynamodb.NewImage.hedge["BOOL"];
    }

    const accountId = message.dynamodb.NewImage.client_order_id["S"];
    const executedAmt = message.dynamodb.NewImage.executed_amount["N"];
    const originalAmt = message.dynamodb.NewImage.original_amount["N"];
    const price = message.dynamodb.NewImage.avg_execution_price["N"];

    // init object if necessary
    initializeObject(accountId, Number(originalAmt) * Number(price)).catch(function(err) {
      if (err.code === 'ConditionalCheckFailedException') {
        console.log('caught and ignoring condtional exception');
      } else {
        throw err;
      }
    }).then(
      // increment with the amounts from this trade
      updateTradeCounts(accountId, type, hedge, executedAmt, price).then(callback(null, {
      statusCode: 200,
      body: JSON.stringify({
        message: 'success!',
        input: event,
      })
    })));

  } else {
    console.log('table type was ' + message.table + ' which is not what we are keyed on');
    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: 'this event was not of type \'Trades\', so we skipped it',
        input: event,
      }),
    };

    callback(null, response);
  }
};
