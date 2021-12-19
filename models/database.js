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
      const friendParams = {
        TableName: "friends",
        Item: {
          "username": {
            "S": username
          },
          "friends": {
            "SS": [username]
          }
        }
      };
      db.putItem(friendParams, function(err, data) {
        if (err) {
          callback(err);
        } else {
          callback(null, "Account successfully created");
        }
      });
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
      },
      "isWall": {
        "S": author
      }
    }
  };
  db.putItem(params, function(err, data) {
    callback(err, data);
  });
}

const makePostToWall = function(author, content, recipient, callback) {
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
      },
      "isWall": {
        "S": recipient
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

const getPostsAndComments = function(user, callback) {
  getFriends(user, function(err, friends) {
    if (err) {
      callback(err, friends);
    } else {
      const promises = [];
      friends.Items.forEach(function(friend) {
        const params = {
          TableName: "posts",
          KeyConditionExpression: "author = :x",
          ExpressionAttributeValues: {
            ":x": {
              "S": friend.user2.S
            }
          }
        };
        promises.push(db.query(params).promise().then(
          function(posts) {
            const postPromises = [];
            posts.Items.forEach(function(item) {
              const key = item.author.S + "$" + item.timestamp.N;
              postPromises.push(getComments(key, function(err, data) {
                if (err) {
                  console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
                } else {
                  item.comments = data;
                }
              }).promise());
            });
            Promise.all(postPromises);
          },
          function(err) {
            console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
          }
        ));
        Promise.all(promises).then(function(data) {
          const ans = [];
          data.forEach((x) => ans.push(x));
          callback(null, ans);
        });
      });
    }
  });
}


// FRIENDS FUNCTIONS

const getFriends = function(user, callback) {
  const params = {
    TableName: "friends",
    KeyConditionExpression: "username = :user",
    ExpressionAttributeValues: {
      ":user": {
        "S": user
      }
    }
  };
  db.query(params, function(err, data) {
    callback(err, data);
  });
}

const addFriend = function(sender, receiver, callback) {
  const params = {
    TableName: "friends",
    Key: {
      "username": {"S": sender}
    },
    UpdateExpression: "ADD friends :y",
    ExpressionAttributeValues: {
      ":y": {"SS": [receiver]}
    },
  };
  db.updateItem(params, function(err, data) {
    if (err) {
      callback(err, data);
    } else {
      const params2 = {
        TableName: "friends",
        Key: {
          "username": {"S": receiver}
        },
        UpdateExpression: "ADD friends :y",
        ExpressionAttributeValues: {
          ":y": {"SS": [sender]}
        },
      };
      db.updateItem(params2, callback);
    }
  });
}

const removeFriend = function(sender, receiver, callback) {
  const params = {
    TableName: "friends",
    Key: {
      "username": {"S": sender}
    },
    UpdateExpression: "DELETE friends :x",
    ExpressionAttributeValues: {
      ":x": {"SS": [receiver]}
    }
  };
  db.updateItem(params, function(err, data) {
    if (err) {
      callback(err, data);
    } else {
      const params2 = {
        TableName: "friends",
        Key: {
          "username": {"S": receiver}
        },
        UpdateExpression: "DELETE friends :x",
        ExpressionAttributeValues: {
          ":x": {"SS": [sender]}
        }
      };
      db.updateItem(params2, callback);
    }
  });
}

const checkFriends = function(user, callback) {
  const params = {
    TableName: "friends",
    KeyConditionExpression: "username = :x",
    ExpressionAttributeValues: {
      ":x": {
        "S": user
      }
    }
  };
  db.query(params, callback);
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
    TableName: "messages2",
    KeyConditionExpression: "groupid = :groupid",
    ExpressionAttributeValues: {
        ':groupid' : {S: String(id)}
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
    TableName: "messages2",
    Item: {
      "groupid": {
        "S": String(groupId)
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
    TableName: "messages2",
    Item: {
      "groupid": {
        "S": String(id)
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

const addRoom = function(username, newRoomName, callback) {

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

      var rooms;

      if (data.Items[0].rooms != null) {
        rooms = data.Items[0].rooms.L

        console.log(rooms);
        rooms.push({"S": newRoomName})
      } else {
        rooms = [{"S": newRoomName}]
      }

      

      console.log(rooms)

      const params = {
        TableName: "users",
        Key: {
          "username": {
            "S": username
          }
        },
        UpdateExpression: "set rooms = :x",
        ExpressionAttributeValues: {
          ":x": {
            "L": rooms
          }
        }
      };
      db.updateItem(params, function(err, data) {
        callback(err, data);
      });


    }
  });

  
}

const addInvite = function(username, recepient, callback) {

  const params = {
    TableName: 'users',
    KeyConditionExpression: '#y = :x',
    ExpressionAttributeNames: {
      '#y': 'username'
    },
    ExpressionAttributeValues: {
      ':x': {
        'S': recepient
      }
    }
  };
  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data);

      var invites;

      if (data.Items[0].chatInvites != null) {
        invites = data.Items[0].chatInvites.L

        console.log(rooms);
        invites.push({"S": username})
      } else {
        invites = [{"S": username}]
      }


      const params = {
        TableName: "users",
        Key: {
          "username": {
            "S": recepient
          }
        },
        UpdateExpression: "set chatInvites = :x",
        ExpressionAttributeValues: {
          ":x": {
            "L": invites
          }
        }
      };
      db.updateItem(params, function(err, data) {
        callback(err, data);
      });


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
  make_post_to_wall: makePostToWall,
  get_posts_and_comments: getPostsAndComments,
  get_posts_by_author: getPostsByAuthor,
  get_friends: getFriends,
  check_friends: checkFriends,
  add_friend: addFriend,
  remove_friend: removeFriend,
  get_comments: getComments,
  make_comment: makeComment,
  get_Messages : getAllMessages,
  add_room : addRoom,
  add_invite : addInvite
};

module.exports = database;