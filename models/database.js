var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

// USERS FUNCTIONS

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
      /*,
      "chats": {
        "L": [ 
          {"N": "1"} , {"N": "2"}, {"N": "7"}
        ]
      }*/
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
    ExpressionAttributeValues: {
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

const changeEmail = function(username, newEmail, callback) {
  const params = {
    TableName: "users",
    Key: {
      "username": {
        "S": username
      }
    },
    UpdateExpression: "set email = :x",
    ExpressionAttributeValues: {
      ":x": {
        "S": newEmail
      }
    }
  };
  db.updateItem(params, function(err, data) {
    callback(err, data);
  });
}

const changePassword = function(username, newPassword, callback) {
  const params = {
    TableName: "users",
    Key: {
      "username": {
        "S": username
      }
    },
    UpdateExpression: "set password = :x",
    ExpressionAttributeValues: {
      ":x": {
        "S": newPassword
      }
    }
  };
  db.updateItem(params, function(err, data) {
    callback(err, data);
  });
}

const changeInterests = function(username, interests, callback) {
  const params = {
    TableName: "users",
    Key: {
      "username": {
        "S": username
      }
    },
    UpdateExpression: "set interests = :x",
    ExpressionAttributeValues: {
      ":x": {
        "SS": interests
      }
    }
  };
  db.updateItem(params, function(err, data) {
    callback(err, data);
  });
}

const searchScan = function(text, callback) {
  const split = text.split(" ");
  var firstname = split[0].toLowerCase();
  var lastname = "";
  firstname = firstname.charAt(0).toUpperCase() + firstname.slice(1);
  if (split.length >= 2) {
    lastname = split[1].toLowerCase();
    lastname = lastname.charAt(0).toUpperCase() + lastname.slice(1);
  }
  const params = {
    TableName: "users",
    ProjectionExpression: "username, firstname, lastname",
    FilterExpression: "contains(firstname, :x) AND contains(lastname, :y)",
    ExpressionAttributeValues: {
      ":x": {
        "S": firstname
      },
      ":y": {
        "S": lastname
      }
    }
  };
  db.scan(params, function(err, data) {
    callback(err, data);
  });
}


// POSTS FUNCTIONS

const makePost = function(author, content, callback) {
  const date = new Date().getTime();
  const params = {
    TableName: "posts",
    Item: {
      "author": {
        "S": author
      },
      "timestamp": {
        "N": date.toString()
      },
      "content": {
        "S": content
      },
      "likes": {
        "N": "0"
      }
    }
  };
  db.putItem(params, function(err, data) {
    callback(err, data);
  });
}

const getPostsByAuthor = function(author, callback) {
  const params = {
    TableName: "posts",
    KeyConditionExpression: "author = :x",
    ExpressionAttributeValues: {
      ":x": {
        "S" : author
      }
    }
  };
  db.query(params, function(err, data) {
    callback(err, data);
  });
}


// FRIENDS FUNCTIONS

const getFriends = function(user, callback) {
  const params = {
    TableName: "friends",
    KeyConditionExpression: "user1 = :user",
    ExpressionAttributeValues: {
      ":user": {
        "S": user
      }
    },
    // FilterExpression: "states = FRIENDS"
  };
  db.query(params, function(err, data) {
    callback(err, data);
  });
}

const addFriend = function(sender, receiver, callback) {
  const params = {
    RequestItems: {
      "friends": [
        {
          PutRequest: {
            Item: {
              "user1": { "S": "sender" },
              "user2": { "S": "receiver" },
              "states": { "S": "FRIENDS" }
            }
          }
        },
        {
          PutRequest: {
            Item: {
              "user1": { "S": "receiver" },
              "user2": { "S": "sender" },
              "states": { "S": "FRIENDS" }
            }
          }
        }
      ]
    }
  };
  db.batchWriteItem(params, function(err, data) {
    callback(err, data);
  });
}

// REACTIONS FUNCTIONS

const getComments = function(key, callback) {
  const params = {
    TableName: "reactions",
    KeyConditionExpression: "authortime = :x",
    ExpressionAttributeValues: {
      ":x": { "S" : key }
    }
  };
  db.query(params, function(err, data) {
    callback(err, data);
  })
}

const makeComment = function(key, content, author, callback) {
  const date = new Date().getTime();
  const params = {
    TableName: "reactions",
    Item: {
      "authortime": {
        "S": key
      },
      "timestamp": {
        "N": date.toString()
      },
      "author": {
        "S": author
      },
      "content": {
        "S": content
      }
    }
  };
  db.putItem(params, function(err, data) {
    callback(err, data);
  });
}


//MESSAGES FUNCTIONS

const getAllMessages = function(id, callback) {
  var params = {
    TableName: "messages",
    KeyConditionExpression: "groupid = :groupid",
    ExpressionAttributeValues: {
        ':groupid' : {N: String(id)}
      },
  };

  db.query(params, function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(null, data);
    }
  });
}

const addMessage = function(author, groupId, message, callback) {
  const date = new Date().getTime();
  const params = {
    TableName: "messages",
    Item: {
      "groupid": {
        "N": String(groupId)
      },
      "timestamp": {
        "S": date.toString()
      },
      "message": {
        "S": message
      },
      "author": {
        "S": author
      }
    }
  };
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

const database = {
  login_lookup: loginLookup,
  add_user: addUser,
  update_affiliation: changeAffiliation,
  update_email: changeEmail,
  update_password: changePassword,
  update_interests: changeInterests,
  search_scan: searchScan,
  addMessage : addMessage,
  deleteMessage : deleteMessage,
  make_post: makePost,
  get_posts_by_author: getPostsByAuthor,
  get_friends: getFriends,
  get_comments: getComments,
  make_comment: makeComment,
  get_Messages : getAllMessages
};

module.exports = database;