var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();


const loginLookup = function(username, callback) {
  const params = {
    TableName: 'users',
    KeyConditionExpression: '#y = :x',
    ExpressionAttributeNames: {
      '#y': 'username'
    },
    ExpressionAttributeValues: {
      ':x': {
        'S': username
      }
    }
  };
  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data);
    }
  });
}

const addUser = function(username, password, firstname, lastname, email, affiliation, birthday, interests, callback) {
  const params = {
    TableName: "users",
    Item: {
      "username": {
        "S": username
      },
      "password": {
        "S": password
      },
      "firstname": {
        "S": firstname
      },
      "lastname": {
        "S": lastname
      },
      "email": {
        "S": email
      },
      "affiliation": {
        "S": affiliation
      },
      "birthday": {
        "S": birthday
      },
      "interests": {
        "SS": interests
      }
    },
    ReturnValues: "NONE"
  }
  db.putItem(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, "Account successfully created");
    }
  });
}

const changeAffiliation = function(username, newAffiliation, callback) {
  const params = {
    TableName: "users",
    Key: {
      "username": {
        "S": username
      }
    },
    UpdateExpression: "set affiliation = :x",
    ExpressionAttributeNames: {
      ":x": {
        "S": newAffiliation
      }
    }
  };
  db.updateItem(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, "Successfully updated database");
    }
  });
}

const addMessage = function(id, timestamp, author, message) {
  const params = {
    TableName: "messages",
    Item: {
      "groupid": {
        "N": id
      },
      "timestamp": {
        "S": timestamp
      },
      "author": {
        "S": author
      },
      "message": {
        "S": message
      }
    }
  }
  db.putItem(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, "Message successfully created");
    }
  });
}


const deleteMessage = function(id, timestamp, author, message) {
  const params = {
    TableName: "messages",
    Item: {
      "groupid": {
        "N": id
      },
      "timestamp": {
        "S": timestamp
      },
      "author": {
        "S": author
      },
      "message": {
        "S": message
      }
    }
  }
  db.deleteItem(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, "Message successfully deleted");
    }
  });
}


const getMessage = function(id) {
  const params = {
    TableName: "messages",
    Item: {
      "groupid": {
        "N": id
      }
    }
  }
  db.Scan(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, "Successfully received all messages");
    }
  });
}


const database = {
  login_lookup: loginLookup,
  add_user: addUser,
  update_affiliation: changeAffiliation,
  addMessage : addMessage,
  deleteMessage : deleteMessage
};

module.exports = database;