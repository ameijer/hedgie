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

var docClient = new AWS.DynamoDB.DocumentClient();

// receive a trigger over SNS, and store in the database
module.exports.register = (event, context, callback) => {

  console.log('event: ' + JSON.stringify(event));
  console.log('context: ' + JSON.stringify(context));

  var message = event.Records[0].Sns.Message;
  console.log('Message received from SNS:', message);

  // obtain trigger object from SNS
  var trigger = JSON.parse(message);
  delete trigger.averages;
  delete trigger.lastPrice;

  saveTriggers(trigger).then(callback(null, {
    statusCode: 200,
    body: JSON.stringify({
      message: 'saved message!',
      input: event,
    }),
  }));
};

// persist a trigger d to Dynamo
function saveTriggers(d) {
  d["lastupdatedts"] = new Date().getTime();
  var saveParams = {
    TableName: "AlertTriggers",
    Item: d
  };

  console.log("saving: " + JSON.stringify(saveParams));

  // return promise
  return docClient.put(saveParams).promise();
}
