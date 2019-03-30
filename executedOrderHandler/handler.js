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

var AWS = require("aws-sdk");
var sns = new AWS.SNS();

AWS.config.update({
  region: "us-east-1"
});

var docClient = new AWS.DynamoDB.DocumentClient();

// handles post processing of executed orders
module.exports.hello = (event, context, callback) => {

  var message = event.Records[0].Sns.Message;
  console.log('Message received from SNS:', message);

  var completedOrder = JSON.parse(message);
  var accountId = completedOrder.accountId;
  const deltaUsd = Number(completedOrder.executed_amount) * Number(completedOrder.avg_execution_price);
  const deltaBTC = Number(completedOrder.original_amount) - Number(completedOrder.remaining_amount);

  console.log('computed deltaUsd: ' + deltaUsd + ', deltaBTC: ' + deltaBTC + ' for account: ' + accountId);

  loadAccountForID(accountId).then(function(data) {
    var account = data.Items[0];
    console.log('account looked up to be: ' + JSON.stringify(account));
    //update account.state
    if (completedOrder.side === 'buy') {
      console.log('handling a buy order');
      account.state = "IN_BTC";

      //decrement insurance fund if needed to cover loss
      account.accountBalanceUSD = Number(account.accountBalanceUSD) - deltaUsd;
      account.accountBalanceBTC = Number(account.accountBalanceBTC) + deltaBTC;

      console.log('about to update target sell price with following params:');
      console.log('account balance USD: ' + account.accountBalanceUSD);
      console.log('account balance BTC: ' + account.accountBalanceBTC);

      console.log('computing fees using price: ' + completedOrder.avg_execution_price);
      var approxAccountValue = account.accountBalanceUSD + (account.accountBalanceBTC * completedOrder.avg_execution_price);

      console.log('approxAccountValue: ' + approxAccountValue);
      var fees = computeFeesForSale(Math.max(0, approxAccountValue + Number(account.profitDelta)));

      console.log('computed fees to be: ' + fees);
      var totalGain = Number(account.profitDelta) + fees;

      console.log('determined total gain over order to be: ' + totalGain);
      var buyAmount = Number(account.accountBalanceBTC) * Number(completedOrder.avg_execution_price);
      console.log('determined buy amount to be: ' + buyAmount);
      var sellAmount = totalGain + buyAmount;
      console.log('sell when the account is worth: ' + sellAmount);
      account.sellPrice = sellAmount / Number(account.accountBalanceBTC);
      console.log('sell price determined to be: ' + account.sellPrice);


      var trigger = {
        "accountId": account.id,
        "sellPrice": account.sellPrice,
        "timestamp": Date.now(),
        "hoursBelowSellPrice": account.hoursToUpdate
      };

      console.log('saving updated account after completion of buy: ' + JSON.stringify(account));
      console.log('adding in trigger after buy: ' + JSON.stringify(trigger));

      //set up hedge trigger
      if (typeof(account.riskFactor) !== 'undefined') {
        console.log('defined riskfactor');
        //use risk factor
        var amtOfMoneyToLose = account.profitUSD * account.riskFactor;
        console.log('we are willing to risk up to ' + account.profitUSD + '*' + account.riskFactor + '=' + amtOfMoneyToLose);
        var minAccountValue = account.targetAmountUsd - amtOfMoneyToLose;
        console.log('minimum acceptable account value is: ' + minAccountValue);
        account.hedgePrice = minAccountValue / account.accountBalanceBTC;
        account.hedgePrice = Math.min(Number(account.hedgePrice), Number(account.sellPrice));

        console.log('computed hedgeprice to be: ' + account.hedgePrice);

        trigger.hedgePrice = account.hedgePrice;
      }
      saveAccount(account).then(submitTriggerToSNS(trigger, callback));

    } else if (completedOrder.side === 'sell') {
      console.log('handling a sell order');
      account.state = "IN_USD";

      account.accountBalanceUSD = Number(account.accountBalanceUSD) + deltaUsd;
      account.accountBalanceBTC = Number(account.accountBalanceBTC) - deltaBTC;

      var profit = Number(account.accountBalanceUSD) - Number(account.targetAmountUsd);
      console.log('profit determined to be: ' + profit);
      if (profit >= 0) {

        console.log('POSITIVE profit determined to be: ' + profit);
        account.profitUSD = Number(account.profitUSD) + profit;
        console.log('account.profitUSD determined to be: ' + account.profitUSD);

        console.log('account.accountBalanceUSD before subtraction of profit: ' + account.accountBalanceUSD);
        account.accountBalanceUSD = account.accountBalanceUSD - profit;
        console.log('account.accountBalanceUSD after subtraction of profit: ' + account.accountBalanceUSD);
      } else {
        console.log('NEGATIVE profit determined to be: ' + profit + ' so siphoning off profits to replenish the USD pool');
        console.log('account profit before decrement: ' + account.profitUSD + ' USD with the fund to replenish having: ' + account.accountBalanceUSD);
        var decrementAmount = Math.min(account.profitUSD, Math.abs(profit));
        console.log('we can siphon up to: ' + decrementAmount + ' USD from the profit without going to negative profit');
        account.profitUSD = Number(account.profitUSD) - decrementAmount;
        account.accountBalanceUSD = account.accountBalanceUSD + decrementAmount;

        console.log('profit successfully siphoned. accountBalanceUSD is now: ' + account.accountBalanceUSD + 'USD, and profitUSD is now: ' + account.profitUSD);
      }
      if (!completedOrder.isHedge) {
        console.log('about to update target buy price with following params:');
        console.log('account balance USD: ' + account.accountBalanceUSD);
        console.log('account balance BTC: ' + account.accountBalanceBTC);
        console.log('computing fees using price: ' + completedOrder.avg_execution_price);

        var fees = computeFeesForSale(Number(account.accountBalanceUSD));
        console.log('computed fees to be: ' + fees);
        var totalMoneyToMake = Number(account.profitDelta) + fees;
        console.log('the total money to make from the upside of the trade: ' + totalMoneyToMake);

        var amtToBuy = Number(account.accountBalanceUSD) / Number(completedOrder.avg_execution_price);
        account.buyPrice = (account.targetAmountUsd - totalMoneyToMake) / amtToBuy;

        console.log('determined buy price to be: ' + account.buyPrice + ': account.buyPrice =' + '(' + account.targetAmountUsd + '+' + totalMoneyToMake + ')' + '/' + amtToBuy);
      } else {
        console.log('this was a hedge sell. not changing the buy price, we will ride it out');
      }
      var trigger = {
        "accountId": account.id,
        "buyPrice": account.buyPrice,
        "timestamp": Date.now(),
        "hoursAboveBuyPrice": account.hoursToUpdate
      };

      console.log('saving updated account after completion of sell: ' + JSON.stringify(account));
      console.log('adding in trigger after sell: ' + JSON.stringify(trigger));
      saveAccount(account).then(submitTriggerToSNS(trigger, callback)).then;
    } else {
      callback({
        "msg": "error. unrecognized side: " + completedOrder.side
      }, null);
    }

  });

  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Go Serverless v1.0! Your function executed successfully!',
      input: event,
    }),
  };
  callback(null, response);
};

function computeFeesForSale(tradevalue) {
  //gemini fees are for bothj maker and taker
  return 0.0025 * tradevalue;

}

function saveAccount(d) {
  var saveParams = {
    TableName: "Accounts",
    Item: d
  };
  console.log("saving: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}

// push on SNS to invoke any listening lambda functions
function submitTriggerToSNS(trigger, callback) {
  //push on SNS
  var params = {
    Message: JSON.stringify(trigger),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:AlertTriggerRegister'
  };
  sns.publish(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      const msg = "successfully published: " + JSON.stringify(params);
      console.log(msg);
      const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: msg
        }),
      };

      callback(null, response);
    } // successful response
  });
}

// scan dynamo for account
function loadAccountForID(accountId) {
  var params = {
    TableName: "Accounts",
    KeyConditionExpression: "#id = :id",
    ExpressionAttributeNames: {
      "#id": "id"
    },
    ExpressionAttributeValues: {
      ":id": accountId
    }
  };

  return docClient.query(params).promise();
}
