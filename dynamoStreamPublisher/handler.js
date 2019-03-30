/**
 * @Author: Alex Meijer <alex>
 * @Date:   05-Aug-2018
 * @Project: hedgie
 * @Filename: handler.js
 * @Last modified by:   alex
 * @Last modified time: 10-Feb-2019
 * @License: See LICENSE file for license terms
 * @Copyright: Copyright 2018 Alex Meijer. All Rights Reserved
 */



'use strict';
var AWS = require("aws-sdk");
var sns = new AWS.SNS();

// set region
AWS.config.update({
  region: "us-east-1"
});

// publish converts a DynamoDB event from an activity stream and pushes
// out to an SNS topic
module.exports.publish = (event, context, callback) => {
  console.log('received event: ' + JSON.stringify(event, null, 2));
  console.log('received dynamodb stream: ' + JSON.stringify(event.Records[0].dynamodb, null, 2));

  var index = 0;

  function next() {
    if (index < event.Records.length) {

      // recursive-style call to loop through array
      publishSingleRecord(event.Records[index++]).then(next);
    }
  }
  next();


  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: 'published: ' + (index) + ' records to stream'
    }),
  };

  callback(null, response);
};

// pushes a single dynamo record to SNS
function publishSingleRecord(message) {

  const arn = message.eventSourceARN;
  message.table = arn.substring(arn.lastIndexOf("table/") + 6, arn.lastIndexOf("/stream"));

  //push on SNS
  var params = {
    Message: JSON.stringify(message),
    /* required */
    MessageStructure: 'raw',
    TopicArn: 'arn:aws:sns:us-east-1:<REDACTED>:DBStream'
  };

  console.log('going to publish: ' + JSON.stringify(params));
  return sns.publish(params).promise();
}
