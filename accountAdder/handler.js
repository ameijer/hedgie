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
var sns = new AWS.SNS();
var retObj = new Object();

// adder function takes parameters passed via API call and creates new
// hedgie bots (accounts)
module.exports.adder = (event, context, callback) => {

  console.log('event: ' + JSON.stringify(event));
  console.log('context: ' + JSON.stringify(context));

  var accountToStore = JSON.parse(event.body);

  console.log('parsed account: ' + JSON.stringify(accountToStore));

  //sanitize input object
  delete accountToStore.id;
  delete accountToStore.accountBalanceBTC;
  delete accountToStore.insuranceFundUSD;
  delete accountToStore.buyPrice;
  delete accountToStore.sellPrice;
  delete accountToStore.hedgePrice;
  delete accountToStore.state;
  delete accountToStore.profitUSD;

  accountToStore.accountBalanceBTC = 0;
  accountToStore.state = 'IN_USD';
  console.log('santized account: ' + JSON.stringify(accountToStore));

  //set price variables to get the bot started
  getNextAccountNumber().then(function(highestAccountNum) {
    console.log('assigning ID: ' + highestAccountNum);
    accountToStore.id = highestAccountNum.toString();
    accountToStore.timestamp = (new Date()).getTime();

    //get current price, set as buy price
    getCurrentPrice().then(function(currentPrice) {
      console.log('setting current account price to be: ' + JSON.stringify(currentPrice));
      accountToStore.buyPrice = currentPrice;

      saveAccount(accountToStore);

      //place initial trigger
      //this is what will be tripped to start the initial buy
      var trigger = new Object();

      trigger.accountId = accountToStore.id;
      trigger.timestamp = (new Date()).getTime();

      if (accountToStore.buyPrice) {
        trigger.buyPrice = accountToStore.buyPrice;
      }

      if (accountToStore.hoursToUpdate) {
        trigger.hoursAboveBuyPrice = accountToStore.hoursToUpdate;
      }

      var params = {
        Message: JSON.stringify(trigger),
        /* required */
        MessageStructure: 'raw',
        TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:AlertTriggerRegister'
      };

      sns.publish(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log("successfully published: " + JSON.stringify(data)); // successful response
      });

      const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Success!'
        }),
      };

      console.log('issuing callback with object: ' + JSON.stringify(response));

      callback(null, response);
    });
  });
};

// this function scans the users stored in dynamo, locates the highest account
// number. It then adds one to supply the next consecutive account
function getNextAccountNumber() {
  return new Promise(function(resolve, reject) {
    var params = {
      TableName: "Accounts"
    };

    retObj.callback = resolve;
    retObj.last = 0;
    docClient.scan(params, onScan);
  });
}

// dynamo scanning helper function
function onScan(err, data) {
  if (err) {
    console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
  } else {
    console.log("Scan succeeded.");
    data.Items.forEach(function(account) {
      console.log("looking at account id: " + account.id);
      if (Number(account.id) > Number(retObj.last)) {
        retObj.last = account.id;
      }
    });

    if (typeof data.LastEvaluatedKey != "undefined") {
      console.log("Scanning for more...");
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      docClient.scan(params, onScan);
    } else {

      console.log("sending highest account number down the chain: " + (Number(retObj.last) + 1));
      // whatever was the higest number we saw during the scan, increment it by one and return it
      // this will be the account number of the account we are trying to add here
      retObj.callback(Number(retObj.last) + 1);
    }
  }
}

// persist an account to Dynamo
function saveAccount(d) {
  var saveParams = {
    TableName: "Accounts",
    Item: d
  };
  console.log("saving: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}

// obtain most recent price to prime new account with
function getCurrentPrice() {

  const recentPriceQuery = {

    TableName: "PriceUpdates",
    KeyConditionExpression: "#typ = :type",
    Limit: 1,
    ScanIndexForward: false,
    ExpressionAttributeNames: {
      "#typ": "type"
    },
    ExpressionAttributeValues: {
      ":type": "BTC/USD",
    }
  };

  // promisify
  return new Promise(function(resolve, reject) {
    docClient.query(recentPriceQuery).promise().then(function(data) {
      console.log('most recent price determined to be ' + JSON.stringify(data));
      var lastPrice = Number(data.Items[0].price);
      console.log('parsed number for lastprice: ' + lastPrice);
      resolve(lastPrice);
    });
  });


}
