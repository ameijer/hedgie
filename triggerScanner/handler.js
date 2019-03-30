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
var AWS = require('aws-sdk');

AWS.config.update({
  region: "us-east-1",
});

var sns = new AWS.SNS();
var docClient = new AWS.DynamoDB.DocumentClient();
var averages = null;
var lastPrice = null;

//set up fixed dynamo query params for most recent price
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

//fixed dynamo query params for most recent price
const recentAverageParams = {

  TableName: "Averages",
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

// somewhat of a hack to store callback globally
function mCallback() {};

// primary serverless function
module.exports.scanner = (event, context, callback) => {
  var params = {
    TableName: "AlertTriggers"
  };

  console.log("Scanning AlertTriggers table.");
  docClient.query(recentPriceQuery).promise().then(function(data) {
    console.log('most recent price determined to be ' + JSON.stringify(data));
    lastPrice = data.Items[0];
  }).then(function() {

    docClient.query(recentAverageParams).promise().then(function(data) {
      averages = data.Items[0];
      docClient.scan(params, onScan);
    }).catch(function(err) {
      console.log(err);
    });

  });
};

// dynamoDB scanning function
function onScan(err, data) {
  if (err) {
    console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
  } else {
    console.log("Scan succeeded." + JSON.stringify(data.Items));
    data.Items.forEach(function(item) {
      console.log('checking ')
      checkTrigger(item);
    });

    // continue to scan if there is additional data
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

// function that checks whether a given trigger registered
// by a bot is 'expired' and should be pushed on the work queue for execution
function checkTrigger(trigger) {

  //if the trigger has been met, push it on the worker SNS
  if (shouldTrigger(trigger)) {
    alertWorkers(trigger);
  }
}

// determines if the trigger criteria has been met
// returns a boolean
function shouldTrigger(trigger) {

  console.log('checking trigger:' + JSON.stringify(trigger));
  console.log('using averages: ' + JSON.stringify(averages));

  //check instant trigger first. are we above sell price or below buy price
  if (lastPrice.price <= trigger.buyPrice || lastPrice.price >= trigger.sellPrice) {
    //we have a buy. delete data to update state of trigger
    delete trigger.hoursBelowSellPrice;
    delete trigger.hoursAboveBuyPrice;
    delete trigger.hedgePrice;
    return true;
  }

  //remove all other triggers on this account if it was a hedge sell
  if (typeof(trigger.hedgePrice) !== 'undefined' && lastPrice.price <= trigger.hedgePrice) {
    //we have a hedge sell. update state of trigger
    console.log('scanned a hedge trigger!');
    delete trigger.hoursBelowSellPrice;
    delete trigger.hoursAboveBuyPrice;
    delete trigger.buyPrice;
    delete trigger.sellPrice;
    return true;
  }

  // if we make it here, then we have not met any 'instant triggers'.
  // check if the trigger has a timed adjustment
  if (typeof(trigger.hoursBelowSellPrice) !== 'undefined') {
    console.log('need to evaluate whether the minimum period has passed for update to sell price range');
    var minUpdateTime = Number(trigger.timestamp) + 1000 * 60 * 60 * Number(trigger.hoursBelowSellPrice);
    var now = new Date().getTime();

    console.log('the current time is: ' + now + ', and we need it to be at least: ' + minUpdateTime + ' for a range trigger update...');
    console.log(minUpdateTime + ' <= ' + now + '?: ' + (minUpdateTime <= now));

    if (minUpdateTime <= now) {
      console.log('we have met minimum time range for delay trigger update')
      console.log('attempting to look at index hoursbelowsellprice: ' + trigger.hoursBelowSellPrice);
      var averageAtTime = averages.averages[trigger.hoursBelowSellPrice.toString()];
      console.log('using: ' + averageAtTime + ' to determine');

      console.log('making comparison: ' + averageAtTime + ">=" + trigger.sellPrice);

      if (averageAtTime <= trigger.sellPrice) {
        console.log('need to update sell levels, as we met criteria for re-adjustment');
        return true;
      }
    } else {
      console.log('did not meet minimum time required to update sell price trigger. remaining ms: ' + (minUpdateTime - now));
    }
  }

  // do we need to adjust up?
  if (typeof(trigger.hoursAboveBuyPrice) !== 'undefined') {
    console.log('need to evaluate whether the minimum period has passed for update to buy price range');
    var minUpdateTime = Number(trigger.timestamp) + 1000 * 60 * 60 * Number(trigger.hoursAboveBuyPrice);
    var now = new Date().getTime();

    console.log('the current time is: ' + now + ', and we need it to be at least: ' + minUpdateTime + ' for a range trigger update...');
    console.log(minUpdateTime + ' <= ' + now + '?: ' + (minUpdateTime <= now));

    if (minUpdateTime <= now) {
      console.log('we have met minimum time range for delay trigger update')
      console.log('attempting to look at index with hoursAboveBuyPrice: ' + trigger.hoursAboveBuyPrice.toString());
      var averageAtTime = averages.averages[trigger.hoursAboveBuyPrice.toString()];
      console.log('using: ' + averageAtTime + ' to determine');

      console.log('making comparison: ' + averageAtTime + ">=" + trigger.buyPrice);

      if (averageAtTime >= trigger.buyPrice) {
        console.log('need to update buy levels, as we met criteria for re-adjustment');
        return true;
      }
    } else {
      console.log('did not meet minimum time required to update buy price trigger. remaining ms: ' + (minUpdateTime - now));
    }
  }

  return false;

}

// push a message on SNS. used to push triggered triggers for processing
function alertWorkers(trigger) {
  if (averages) {
    trigger["averages"] = averages;
  }

  if (lastPrice) {
    trigger["lastPrice"] = lastPrice;
  }

  var params = {
    Message: JSON.stringify(trigger),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:WorkAlert'
  };

  sns.publish(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
    } else {
      console.log("successfully published: " + JSON.stringify(params));
      deleteTrigger(trigger);
    } // successful response
  });

}

// deletes a trigger from the corresponding dynamoDB table
function deleteTrigger(trigger) {
  var params = {
    TableName: "AlertTriggers",
    Key: {
      "accountId": trigger.accountId,
      "timestamp": trigger.timestamp
    }
  };

  docClient.delete(params, function(err, data) {
    if (err) {
      console.error("Unable to delete item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("DeleteItem succeeded:", JSON.stringify(data, null, 2));
      console.log("workers alerted for trigger: " + JSON.stringify(trigger));
    }
  });
}
