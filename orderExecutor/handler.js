/**
 * @Author: Alex Meijer <alex>
 * @Date:   05-Aug-2018
 * @Project: hedgie
 * @Filename: handler.js
 * @Last modified by:   alex
 * @Last modified time: 18-Nov-2018
 * @License: See LICENSE file for license terms
 * @Copyright: Copyright 2018 Alex Meijer. All Rights Reserved
 */

'use strict';

var AWS = require("aws-sdk");
var request = require('sync-request');
var cryptoJS = require("crypto-js");
var sns = new AWS.SNS();
const GEMINI_FEE = 0.0025;

AWS.config.update({
  region: "us-east-1"
});

var docClient = new AWS.DynamoDB.DocumentClient();

// a mock trading function. Were hedgie ever to be wired to an actual API,
// this code would haveto make the calls to place that exchange order
// here, we just make some quick calculations to simulate an instant, complete
// fulfillment of the order
module.exports.hello = (event, context, callback) => {

  console.log('event: ' + JSON.stringify(event));
  console.log('context: ' + JSON.stringify(context));

  var message = event.Records[0].Sns.Message;
  console.log('Message received from SNS:', message);

  var order = JSON.parse(message);
  var account = order.account;
  var trigger = order.trigger;
  var tradePromise;

  //place order for immediate or cancel
  if (account.state === "BUYING_BTC") {
    //for Buying
    //when bought, notify executor to update balances, set triggers
    tradePromise = executeBuyOrder(account, Number(trigger.lastPrice.price));
  } else if (account.state === "SELLING_BTC") {

    // when selling, act differently when this is a hedging action vs a profit
    // sell
    if (typeof(trigger.hedgePrice) !== 'undefined') {
      console.log('hedge sell!')
      tradePromise = executeSellOrder(account, Number(trigger.lastPrice.price), true);
    } else {
      tradePromise = executeSellOrder(account, Number(trigger.lastPrice.price), false);
    }
  } else {
    console.log("ERROR - INVALID ACCOUNT STATE FOR EXECUTOR: " + account.state);
    callback("ERROR - INVALID ACCOUNT STATE FOR EXECUTOR: " + account.state, null);
  }

  // promise chain to exec the trade, then notify other parts of the bot
  tradePromise.then(function(trade) {
    saveTrade(trade).then(pushNotification(trade, callback));
  });

};

// calculates the anticipated trading fees
function feesAtPrice(amtBitcoinTraded, price) {
  console.log('running fee estimator with params: amtBitcoinTraded: ' + amtBitcoinTraded + ', price: ' + price);
  var feesUSD = GEMINI_FEE * Number(amtBitcoinTraded) * Number(price);
  var feesBTC = feesUSD / Number(price);
  console.log('I think we paid: ' + feesUSD + ' USD for this trade, or ' + feesBTC + ' BTC');

  //keep all fees in BTC for ease of accounting
  return feesBTC;
}


function executeSellOrder(account, price, isHedge) {

  if (account.type === 'IMAGINARY') {
    var promise = new Promise(function(resolve, reject) {

      var result = {
        // These are the same fields returned by order/status
        "order_id": "22333",
        "client_order_id": account.id,
        "acctType": account.type,
        "symbol": "btcusd",
        "price": price,
        "avg_execution_price": price,
        "side": "sell",
        "type": "exchange limit",
        "timestamp": Date.now(),
        "timestampms": 128938491234,
        "is_live": true,
        "is_cancelled": false,
        "options": ["maker-or-cancel"],
        "executed_amount": Number(account.accountBalanceBTC) - feesAtPrice(account.accountBalanceBTC, price),
        "remaining_amount": "0",
        "original_amount": Number(account.accountBalanceBTC) - feesAtPrice(account.accountBalanceBTC, price),
        "accountId": account.id,
        "hedge": isHedge,
        "accountId": account.id,
        "notificationType": "TRADE"
      }
      console.log('created mock completed sell: ' + JSON.stringify(result));
      resolve(result);
    });

    return promise;
  } else if (account.type === 'GEMINI_SANDBOX') {
    var promise = new Promise(function(resolve, reject) {

      var result = executeGeminiSell(account, true);
      console.log('result of sandboxed gemini sell: ' + JSON.stringify(result));
      resolve(result);
    });

    return promise;
  } else {
    throw 'account.type: ' + account.type + ' is not supported for sales';
  }
}

function executeBuyOrder(account, price) {
  if (account.type === 'IMAGINARY') {
    var promise = new Promise(function(resolve, reject) {
      var result = {
        // These are the same fields returned by order/status
        "order_id": "22333",
        "client_order_id": account.id,
        "acctType": account.type,
        "symbol": "btcusd",
        "price": price,
        "avg_execution_price": price,
        "side": "buy",
        "type": "exchange limit",
        "timestamp": Date.now(),
        "timestampms": 128938491234,
        "is_live": true,
        "is_cancelled": false,
        "options": ["maker-or-cancel"],
        "executed_amount": (account.accountBalanceUSD / price) - feesAtPrice(account.accountBalanceBTC, price),
        "remaining_amount": "0",
        "original_amount": (account.accountBalanceUSD / price) - feesAtPrice(account.accountBalanceBTC, price),
        "accountId": account.id,
        "notificationType": "TRADE"
      }
      console.log('created mock completed buy: ' + JSON.stringify(result));
      resolve(result);
    });

    return promise;
  } else if (account.type === 'GEMINI_SANDBOX') {
    var promise = new Promise(function(resolve, reject) {

      var result = executeGeminiBuy(account, true);
      console.log('result of sandboxed gemini buy: ' + JSON.stringify(result));
      resolve(result);
    });

    return promise;
  } else {
    throw 'account.type: ' + account.type + ' is not supported for sales';
  }
}

function saveTrade(d) {
  var saveParams = {
    TableName: "Trades",
    Item: d
  };
  console.log("saving: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}

function pushNotification(tradeInfo, callback) {
  //push on SNS
  var params = {
    Message: JSON.stringify(tradeInfo),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:TradeNotifications'
  };
  sns.publish(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      console.log("successfully published: " + JSON.stringify(params));
      const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Go Serverless v1.0! Your function executed successfully!'
        }),
      };

      callback(null, response);
    } // successful response
  });
}

//to keep this app stateless, we don't leave any orders on the book.
//to sell, then, we sell at a low price because immediate or cancel will
//pick the best price for us anyway
// say, 5% below the price we triggered on
function executeGeminiSell(account, isSandboxed) {
  var sellLowerBound = account.sellPrice * 0.95;
  var requestBody = {
    "request": "/v1/order/new",
    "nonce": new Date().getTime(),

    "client_order_id": account.id + '',
    "symbol": "btcusd",
    "amount": account.accountBalanceBTC + '',
    "price": sellLowerBound + '',
    "side": "sell",
    "type": "exchange limit",
    "options": ["immediate-or-cancel"]
  };

  console.log('final form of request body: ' + JSON.stringify(requestBody, null, 4));
  var url = null;
  if (isSandboxed) {
    url = 'https://api.sandbox.gemini.com/v1/order/new';
    console.log('using sandbox URL: ' + url);
  } else {
    url = 'https://api.gemini.com/v1/order/new';
    console.log('using actual exchange URL: ' + url);
  }

  return makeAPICall(requestContentObject, account, url);
}

function makeAPICall(requestContentObject, account, url) {
  console.log('making API call to url: ' + url);
  var payload = new Buffer(JSON.stringify(requestContentObject)).toString('base64')
  var res = request('POST', url, {
    'headers': {
      'Cache-Control': 'no-cache',
      'Content-Length': '0',
      'Content-Type': 'text/plain',
      'X-GEMINI-APIKEY': account.api_key,
      'X-GEMINI-PAYLOAD': payload,
      'X-GEMINI-SIGNATURE': cryptoJS.HmacSHA384(payload, account.secret)
    }
  });

  console.log('got response back: ' + JSON.stringify(res, null, 4));

  var response = JSON.parse(res.getBody('utf8'));
  console.log('extracted response body: ' + JSON.strigify(response));
  return response;
}

//to keep this app stateless, we don't leave any orders on the book.
//to sell, then, we sell at a high price because immediate or cancel will
//pick the best price for us anyway
// say, 5% above the price we triggered on
function executeGeminiBuy(account, isSandboxed) {
  var buyUpperBound = account.buyPrice * 1.05;
  var amountToBuy = account.accountBalanceUSD / account.buyPrice;
  var requestBody = {
    "request": "/v1/order/new",
    "nonce": new Date().getTime(),

    "client_order_id": account.id + '',
    "symbol": "btcusd",
    "amount": amountToBuy + '',
    "price": buyUpperBound + '',
    "side": "buy",
    "type": "exchange limit",
    "options": ["immediate-or-cancel"]
  };

  console.log('final form of request body: ' + JSON.stringify(requestBody, null, 4));
  var url = null;
  if (isSandboxed) {
    url = 'https://api.sandbox.gemini.com/v1/order/new';
    console.log('using sandbox URL: ' + url);
  } else {
    url = 'https://api.gemini.com/v1/order/new';
    console.log('using actual exchange URL: ' + url);
  }

  return makeAPICall(requestContentObject, account, url);
}
