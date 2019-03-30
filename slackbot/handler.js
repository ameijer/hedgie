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

const AWS = require('aws-sdk');
const url = require('url');
const https = require('https');

// The base-64 encoded, encrypted key (CiphertextBlob) stored in the kmsEncryptedHookUrl environment variable
//const kmsEncryptedHookUrl = process.env.kmsEncryptedHookUrl;

// The Slack channel to send a message to stored in the slackChannel environment variable
const slackChannel = "hedgie";
let hookUrl = "https://hooks.slack.com/<REDACTED>";

var docClient = new AWS.DynamoDB.DocumentClient();

// make the call to slack to post the message
function postMessage(message, callback) {
  const body = JSON.stringify(message);
  const options = url.parse(hookUrl);
  options.method = 'POST';
  options.headers = {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
  };

  const postReq = https.request(options, (res) => {
    const chunks = [];
    res.setEncoding('utf8');

    // support chunked encoding
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      if (callback) {
        callback({
          body: chunks.join(''),
          statusCode: res.statusCode,
          statusMessage: res.statusMessage,
        });
      }
    });
    return res;
  });

  postReq.write(body);
  postReq.end();
}

// Helper function to make the actual slack request
function processEvent(event, callback) {
  const obj = JSON.parse(event.Records[0].Sns.Message);
  console.log('in processevent, obj is: ' + JSON.stringify(obj));
  //provide a way to directly send a message
  if (typeof obj.slackMessage !== 'undefined') {
    var slackMessage = obj.slackMessage;

    console.log('sending following slackMessage: ' + JSON.stringify(slackMessage));
    postMessage(slackMessage, (response) => {
      if (response.statusCode < 400) {
        console.info('Message posted successfully');
        callback(null);
      } else if (response.statusCode < 500) {
        console.error(`Error posting message to Slack API: ${response.statusCode} - ${response.statusMessage}`);
        callback(null); // Don't retry because the error is due to a problem with the request
      } else {
        // Let Lambda retry
        callback(`Server error when processing message: ${response.statusCode} - ${response.statusMessage}`);
      }
    });

  } else {
    obtainAccount(obj.client_order_id, function(account) {
      var trade = obj;
      console.log('got trade: ' + JSON.stringify(trade));
      var color, fallback, title;
      var fields = [];
      if (trade.hedge) {
        //loss
        color = "danger";
        fallback = "HedgieBot " + trade.client_order_id + " hedged @ " + trade.avg_execution_price;
        title = "HedgieBot " + trade.client_order_id + " has hedged @" + trade.avg_execution_price;
        fields.push({
          "title": "Fund Value",
          "value": '$' + Number(account.accountBalanceUSD).toFixed(2) + ' / ' + '$' + Number(account.targetAmountUsd).toFixed(2),
          "short": false
        });
        fields.push({
          "title": "Siphoned Profit",
          "value": '$' + Number(account.profitUSD).toFixed(2),
          "short": true
        });
      } else if (trade.side === 'sell') {
        //profit
        color = "good";
        fallback = "HedgieBot " + trade.client_order_id + " sold for a profit @ " + trade.avg_execution_price;
        title = "HedgieBot " + trade.client_order_id + " has profited @ " + trade.avg_execution_price + "!! Congrats!";
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
      } else {
        //buy
        color = "#439FE0";
        fallback = "HedgieBot " + trade.client_order_id + " bought @ " + trade.avg_execution_price;
        title = "HedgieBot " + trade.client_order_id + " has bought @ " + trade.avg_execution_price;
        if (typeof(account.hoursToUpdate) !== 'undefined') {
          fields.push({
            "title": "Activity Setting",
            "value": account.hoursToUpdate + '',
            "short": true
          });
        }
      }

      if (typeof(account.riskFactor) !== 'undefined') {
        fields.push({
          "title": "Risk Factor",
          "value": account.riskFactor + '',
          "short": true
        });
      }

      const slackMessage = {
        channel: slackChannel,
        attachments: [{
          "fallback": fallback,
          "color": color,
          "title": title,
          "fields": fields,
          "ts": Math.round((new Date()).getTime() / 1000)
        }]
      };

      console.log('sending following slackMessage: ' + JSON.stringify(slackMessage));
      postMessage(slackMessage, (response) => {
        if (response.statusCode < 400) {
          console.info('Message posted successfully');
          callback(null);
        } else if (response.statusCode < 500) {
          console.error(`Error posting message to Slack API: ${response.statusCode} - ${response.statusMessage}`);
          callback(null); // Don't retry because the error is due to a problem with the request
        } else {
          // Let Lambda retry
          callback(`Server error when processing message: ${response.statusCode} - ${response.statusMessage}`);
        }
      });
    });
  }
}

// Lookup method in dynamo for accounts
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

// delay execution X ms
function sleep(delay) {
  var start = new Date().getTime();
  // simple spin lock to psuedosleep
  while (new Date().getTime() < start + delay);
}

// receive a message on SNS, and publish a slack message based on its contents
module.exports.slackPublish = (event, context, callback) => {

  if (hookUrl) {
    processEvent(event, callback);
  } else if (kmsEncryptedHookUrl && kmsEncryptedHookUrl !== '<kmsEncryptedHookUrl>') {
    const encryptedBuf = new Buffer(kmsEncryptedHookUrl, 'base64');
    const cipherText = {
      CiphertextBlob: encryptedBuf
    };

    const kms = new AWS.KMS();
    kms.decrypt(cipherText, (err, data) => {
      if (err) {
        console.log('Decrypt error:', err);
        return callback(err);
      }
      hookUrl = `https://${data.Plaintext.toString('ascii')}`;
      processEvent(event, callback);
    });
  } else {
    callback('Hook URL has not been set.', null);
  }
};
