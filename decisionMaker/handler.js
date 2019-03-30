/**
 * @Author: Alex Meijer <alex>
 * @Date:   10-Feb-2019
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

// this module takes triggers that have been met, i.e. price action
// has exceeded their thresholds

// it determines whether the trigger can get re-set, or needs to be acted upon
module.exports.compute = (event, context, callback) => {

  var message = event.Records[0].Sns.Message;
  console.log('Message received from SNS:', message);

  var trigger = JSON.parse(message);
  console.log('trigger parsed to be: ' + JSON.stringify(trigger));

  //obtain account information for the tripped trigger
  obtainAccountFor(trigger, function(account) {
    var shouldReset = false;
    console.log('account for trigger loaded: ' + JSON.stringify(account));
    if (typeof(trigger.hoursAboveBuyPrice) !== 'undefined' || typeof(trigger.hoursBelowSellPrice) !== 'undefined') {
      console.log('trigger met criteria: trigger.hoursAboveBuyPrice || trigger.hoursBelowSellPrice, updating ranges');
      //set period trigger based on risk setting to follow trends and update price ranges
      //install single time range trigger, then update all client triggers if the got left way back
      //range trigger controlled by slider <-this is the risk setting (30 days much safer than 12 hrs)
      //when a range trigger is tripped, it updates
      updateRanges(trigger, account, callback);

    } else if (typeof(trigger.sellPrice) !== 'undefined' && Number(trigger.sellPrice) <= Number(trigger.lastPrice.price)) {
      console.log('trigger met criteria: typeof(trigger.sellPrice) !== undefined && trigger.sellPrice <= trigger.lastPrice.price, selling');
      //sell
      placeSellOrder(account, trigger, callback);

    } else if (typeof(trigger.hedgePrice) !== 'undefined' && trigger.hedgePrice >= Number(trigger.lastPrice.price)) {
      console.log('trigger met criteria: typeof(trigger.hedgePrice) !== undefined && trigger.hedgePrice >= trigger.lastPrice.price, selling');
      // stop-loss (hedge) order

      if (typeof(account.hedgeDelayMinutes) !== 'undefined') {
        var canHedgeAfter = Number(trigger.timestamp) + (Number(account.hedgeDelayMinutes) * 60 * 1000);
        console.log('minimum hedge delay detected.');

        if (canHedgeAfter <= Date.now()) {
          console.log('we determined we have been below the hedge price for ' + account.hedgeDelayMinutes + ' min. we could hedge after: ' + canHedgeAfter + ', and right now the time is: ' + Date.now());
          shouldReset = false;
        } else {
          console.log('not long enough has passed to hedge. remaining ms: ' + (canHedgeAfter - Date.now()));
          shouldReset = true;
          trigger.sellPrice = account.sellPrice;
          trigger.hoursBelowSellPrice = account.hoursToUpdate;
        }
      }

      if (!shouldReset) {
        console.log('not resetting trigger, placing a hedge sell order');
        if (typeof(account.hedgeTimes) === 'undefined') {
          account.hedgeTimes = [];
        }

        account.hedgeTimes.push(new Date().toISOString());
        console.log('hedgetimes after push: ' + JSON.stringify(account.hedgeTimes));
        //sell
        placeSellOrder(account, trigger, callback);
      }

    } else if (typeof(trigger.buyPrice) !== 'undefined' && Number(trigger.buyPrice) >= Number(trigger.lastPrice.price)) {
      console.log('trigger met criteria: typeof(trigger.buyPrice) !== undefined && trigger.buyPrice >= trigger.lastPrice.price), buying');
      //a normal buy
      placeBuyOrder(account, trigger, callback);
    } else {
      console.log('no conditions met. resetting trigger')
      //Reset trigger, let it trip again and we will verify again
      shouldReset = true;
    }

    if (shouldReset) {

      resetTrigger(trigger, callback);
    }

  });
};

// re-submit trigger for storage - false alarm
function resetTrigger(trigger, callback) {
  //publish back to SNS
  //arn:aws:sns:us-east-1:<REDACTED>:AlertTriggerRegister
  delete trigger.averages;
  delete trigger.lastPrice;
  var params = {
    Message: JSON.stringify(trigger),
    /* required */
    MessageStructure: 'raw',

    // push to lambda that stores items back on the trigger table
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:AlertTriggerRegister'
  };

  console.log('submitting trigger reset message: ' + JSON.stringify(params));
  sns.publish(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
    } else {
      console.log("successfully published: " + JSON.stringify(data));
      callback(null, {
        "result": "successfully published: " + JSON.stringify(data)
      });
    } // successful response
  });

}

// handler for when triggers are tripped by going out of Date
// update
function updateRanges(trigger, account, callback) {
  //update account object
  console.log('updating trigger: ' + JSON.stringify(trigger));
  var hoursBack;

  if (trigger.hoursAboveBuyPrice) {
    hoursBack = trigger.hoursAboveBuyPrice;
  } else {
    hoursBack = trigger.hoursBelowSellPrice;
  }

  console.log('determined hoursback to be: ' + hoursBack);

  // set trigger level based on account settings
  var avgBTCPrice = trigger.averages.averages[hoursBack];
  console.log('the average price ' + hoursBack + ' hours ago is: ' + avgBTCPrice);

  updatePrices(account, Number(avgBTCPrice));

  saveAccount(account).then(
    //update single buy trigger to reflect new buyprice
    setTriggers(account, function() {

      callback(null, {
        "message": "sucessfully reached terminiation of updateranges," +
         "updated account info that was saved: " + JSON.stringify(account)
      });

    })
  );
}

// re-computer prices
function updatePrices(account, lastPrice) {
  console.log('about to update prices on account: '
    + JSON.stringify(account) + ' using price: ' + JSON.stringify(lastPrice));

  // custAmtBtc * sellPrice = (cust amt of btc * buyprice) + profitDelta
  if (account.state === 'IN_BTC') {

    console.log('about to update target sell price with following params:');
    console.log('account balance USD: ' + account.accountBalanceUSD);
    console.log('account balance BTC: ' + account.accountBalanceBTC);
    console.log('computing fees using price: ' + lastPrice);
    var approxAccountValue = account.accountBalanceUSD + (account.accountBalanceBTC * lastPrice);
    console.log('approxAccountValue: ' + approxAccountValue);
    var fees = computeFeesForSale(Math.max(0, approxAccountValue + Number(account.profitDelta)));
    console.log('computed fees to be: ' + fees);
    var totalGain = Number(account.profitDelta) + fees;
    console.log('determined total gain over order to be: ' + totalGain);
    var buyAmount = Number(account.accountBalanceBTC) * lastPrice;
    console.log('determined buy amount to be: ' + buyAmount);
    var sellAmount = totalGain + buyAmount;
    console.log('sell when the account is worth: ' + sellAmount);
    account.sellPrice = sellAmount / Number(account.accountBalanceBTC);
    console.log('account.sellPrice: ' + account.sellPrice);
    if (typeof(account.hedgePrice) !== 'undefined') {
      account.hedgePrice = Math.min(Number(account.hedgePrice), Number(account.sellPrice));
    }
  }

  if (account.state === 'IN_USD') {
    var fees = computeFeesForSale(Number(account.accountBalanceUSD));
    console.log('computed fees to be: ' + fees);
    var totalMoneyToMake = Number(account.profitDelta) + fees;
    console.log('the total money to make from the upside of the trade: ' + totalMoneyToMake);

    var amtToBuy = Number(account.accountBalanceUSD) / Number(lastPrice);
    account.buyPrice = (account.targetAmountUsd - totalMoneyToMake) / amtToBuy;
    console.log('determined buy price to be: ' + account.buyPrice + ': account.buyPrice =' + '(' + account.targetAmountUsd + '+' + totalMoneyToMake + ')' + '/' + amtToBuy);

  }

  console.log('state of account after price updates: ' + JSON.stringify(account));

}

function computeFeesForSale(tradevalue) {
  //gemini fees are for bothj maker and taker
  return 0.0025 * tradevalue;

}

// create new triggers based off of account settings
function setTriggers(account, callback) {

  var targetTrigger = new Object();
  //set the trigger for at or below the buy price and above the hedgeprice
  if (account.state === 'IN_USD') {
    console.log('account state in usd. going to add a buy trigger');
    targetTrigger.buyPrice = account.buyPrice;
    targetTrigger.accountId = account.id;
    targetTrigger.timestamp = new Date().getTime();
    targetTrigger.hoursAboveBuyPrice = account.hoursToUpdate;
  }

  if (account.state === 'IN_BTC') {
    console.log('account state in BTC. going to add a sell trigger');
    targetTrigger.sellPrice = account.sellPrice;
    targetTrigger.accountId = account.id;
    targetTrigger.hedgePrice = account.hedgePrice;
    targetTrigger.timestamp = new Date().getTime();
    targetTrigger.hoursBelowSellPrice = account.hoursToUpdate;
  }

  console.log('updateTriggers - state of tigger  AFTER update: ' + JSON.stringify(targetTrigger));
  saveTrigger(targetTrigger).then(callback());

}

// persist trigger to database
function saveTrigger(trig) {
  trig["timestamp"] = new Date().getTime();
  var saveParams = {
    TableName: "AlertTriggers",
    Item: trig
  };
  console.log("saving trigger with following params: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}


function saveAccount(d) {
  var saveParams = {
    TableName: "Accounts",
    Item: d
  };
  console.log("saving account: " + JSON.stringify(saveParams));
  return docClient.put(saveParams).promise();
}

function obtainAccountFor(trigger, callback) {
  var params = {
    TableName: "Accounts",
    KeyConditionExpression: "#iden = :id",
    ExpressionAttributeNames: {
      "#iden": "id"
    },
    ExpressionAttributeValues: {
      ":id": trigger.accountId
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

function placeBuyOrder(account, trigger, callback) {
  account.state = 'BUYING_BTC';

  var params = {
    Message: JSON.stringify({
      "trigger": trigger,
      "account": account
    }),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:Orders'
  };

  console.log('publishing buy order on SNS: ' + JSON.stringify(params));
  saveAccount(account).then(sns.publish(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
    } else {
      console.log("successfully published: " + JSON.stringify(data));
      const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Go Serverless v1.0! Your function executed successfully!'
        })
      };

      callback(null, response);
    } // successful response
  }));
}

function placeSellOrder(account, trigger, callback) {
  //update account: status is SELLING
  account.state = 'SELLING_BTC';

  var params = {
    Message: JSON.stringify({
      "trigger": trigger,
      "account": account
    }),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:Orders'
  };

  console.log('publishing sell order on SNS: ' + JSON.stringify(params));
  saveAccount(account).then(sns.publish(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
    } else {
      console.log("successfully published: " + JSON.stringify(data));
      const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Go Serverless v1.0! Your function executed successfully!'
        })
      };
      callback(null, response);
    } // successful response
  }));


}
