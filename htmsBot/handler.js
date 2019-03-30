/**
 * @Author: Alex Meijer <alex>
 * @Date:   05-Aug-2018
 * @Project: hedgie
 * @Filename: handler.js
 * @Last modified by:   alex
 * @Last modified time: 03-Mar-2019
 * @License: See LICENSE file for license terms
 * @Copyright: Copyright 2018 Alex Meijer. All Rights Reserved
 */



'use strict';
var AWS = require('aws-sdk');

AWS.config.update({
  region: "us-east-1",
});

var sns = new AWS.SNS();
var docClient = new AWS.DynamoDB.DocumentClient();
var averages = null;
var lastPrice = null;

// table containing metrics to scan and publish
const params = {
  TableName: "Metrics"
};

function mCallback() {};

// this is run on a timer. As soon as the handler is invoked, begin scanning
// the analytics tables for submission to slack
module.exports.hello = (event, context, callback) => {
  mCallback = callback;
  docClient.scan(params, onScan);
};

function onScan(err, data) {
  if (err) {
    console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
  } else {
    console.log("Scan succeeded." + JSON.stringify(data.Items));
    data.Items.forEach(function(item) {
      console.log('checking: ' + JSON.stringify(item));

      // for each item in the table
      submitMetric(item);
    });

    // continue scanning if we have more movies, because
    // scan can retrieve a maximum of 1MB of data
    if (typeof data.LastEvaluatedKey != "undefined") {
      console.log("Scanning for more...");
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      docClient.scan(params, onScan);
    }

    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: 'ran ok',
        input: data,
      }),
    };
    mCallback(null, response);
  }
}

// build a slackbot message, then push it
function submitMetric(metric) {

  obtainAccount(metric.accountId, function(account) {
    var title = "HTMS Analytics for bot:  " + metric.accountId;
    var fields = [];
    fields.push({
      "title": "Total Trades",
      "value": metric.totalTrades,
      "short": true
    });

    fields.push({
      "title": "Buys",
      "value": metric.buys + '',
      "short": true
    });

    fields.push({
      "title": "Sells",
      "value": metric.sells + '',
      "short": true
    });

    fields.push({
      "title": "Hedges",
      "value": metric.hedges + '',
      "short": true
    });

    fields.push({
      "title": "Volume (BTC)",
      "value": metric.volumeBTC.toFixed(4) + '',
      "short": true
    });

    fields.push({
      "title": "Volume (USD)",
      "value": metric.volumeUSD.toFixed(4) + '',
      "short": true
    });

    fields.push({
      "title": "Exchange Balance (BTC)",
      "value": metric.exchangeBalanceBTC.toFixed(4) + '',
      "short": true
    });

    fields.push({
      "title": "Exchange Balance (USD)",
      "value": metric.exchangeBalanceUSD.toFixed(4) + '',
      "short": true
    });

    fields.push({
      "title": "Fund Value",
      "value": '$' + Number(account.accountBalanceUSD).toFixed(2) + ' / ' + '$' + Number(account.targetAmountUsd).toFixed(2),
      "short": false
    });

    fields.push({
      "title": "Siphoned Profit",
      "value": '$' + Number(account.profitUSD).toFixed(2) + '',
      "short": true
    });

    const slackMessage = {
      channel: "hedgie",
      attachments: [{
        "fallback": title,
        "color": "#838996",
        "title": title,
        "fields": fields,
        "ts": Math.round((metric.lastUpdated / 1000))
      }]
    };

    var params = {
      Message: JSON.stringify({
        "slackMessage": slackMessage
      }),
      /* required */
      MessageStructure: 'raw',
      TopicArn: 'arn:aws:sns:<REDACTED>:SlackBot'
    };

    sns.publish(params, function(err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
      } else {
        console.log("successfully published: " + JSON.stringify(params));
      } // successful response
    });
  });
}

// query db for specific account
function obtainAccount(idNum, callback) {
  var params = {
    TableName: "Accounts",
    KeyConditionExpression: "#iden = :id",
    ExpressionAttributeNames: {
      "#iden": "id"
    },
    ExpressionAttributeValues: {
      ":id": idNum
    }
  };

  console.log('querying for account number with following query: ' + JSON.stringify(params));
  docClient.query(params, function(err, data) {
    if (err) {
      console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      console.log('invoking callback with param: ' + JSON.stringify(data.Items[0]));
      callback(data.Items[0]);
    }
  });
}
