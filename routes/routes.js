const fs = require("fs");
const crypto = require("crypto");
var db = require('../models/database.js');
const AWS = require("aws-sdk");
const { resolveSoa } = require("dns");
const { check_friends } = require("../models/database.js");

const renderLogin = function(req, res) {
  if (req.session.username) {
    res.redirect("/feed");
  } else {
    res.render("login.ejs", {errMsg: null});
  }
}

const renderSignup = function(req, res) {
  res.render("signup.ejs", {});
}

const renderWall = function(req, res) {
  const page = req.url.substring(15);
  if (req.session.username === page) {
    res.render("wall_self.ejs", {});
  } else if (req.session.username) {
    db.check_friends(req.session.username, function(err, data) {
      if (err) {
        console.error(JSON.stringify(err, null, 2));
      } else {
        const friends = data.Items[0].friends.SS;
        if (friends && friends.includes(page)) {
          db.log_last_action(req.session.username, function(err, data) {
            if (err) {
              console.error(JSON.stringify(err, null, 2));
            } else {
              res.render("wall_friend.ejs", {});
            }
          });
        } else {
          db.log_last_action(req.session.username, function(err, data) {
            if (err) {
              console.error(JSON.stringify(err, null, 2));
            } else {
              res.render("wall_stranger.ejs", {});
            }
          });
        }
      }
    });
  } else {
    res.redirect("/");
  }
}

const getUser = function(req, res) {
  const username = req.body.username;
  db.login_lookup(username, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      res.send({
        success: true,
        data: data.Items[0]
      });
    }
  });
}

const getLastAction = function(req, res) {
  const username = req.body.username;
  db.get_last_action(username, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      const lastAction = data.Items[0].lastAction;
      if (!lastAction) {
        res.send({
          success: true,
          data: false
        });
      } else {
        const currentTime = new Date();
        if (currentTime.getTime() - lastAction.N <= 300000) {
          res.send({
            success: true,
            data: true
          });
        } else {
          res.send({
            success: true,
            data: false
          });
        }
      }
    }
  });
}

const getFriends = function(req, res) {
  db.get_friends(req.session.username, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      res.send({
        success: true,
        data: data.Items[0]
      });
    }
  });
}

const checkLogin = function(req, res) {
  const username = req.body.username;
  const password = req.body.password;
  if (username.length === 0 || password.length === 0) {
    res.send({
      success: false,
      msg: "All input fields need to be completed!"
    });
  } else {
    const hashed = crypto.createHash("sha256").update(password).digest("hex");
    db.login_lookup(username, function(err, data) {
      if (err) { // No data was returned (username is not in database)
        res.send({
          success: false,
          msg: "This username/password combination was not found"
        });
      } else if (data && data.Items[0].password.S === hashed) { // Username was found in table and password is a match
          req.session.username = username; // Creates username in the session
          db.log_last_action(req.session.username, function(err, data) {
            if (err) {
              console.error(JSON.stringify(err, null, 2));
            } else {
              res.send({
                success: true,
                msg: null
              });
            }
          });
      } else {
        res.send({
          success: false,
          msg: "This username/password combination was not found"
        });
      }
    });
  }
}

const getFeed = function(req, res) {
  if (req.session.username) {
    db.log_last_action(req.session.username, function(err, data) {
      if (err) {
        console.error(JSON.stringify(err, null, 2));
      } else {
        res.render("main.ejs", {});
      }
    });
  } else {
    res.redirect("/login");
  }
}

const signupUser = function(req, res) {
  const username = req.body.username;
  const password = req.body.password;
  const firstname = req.body.firstname;
  const lastname = req.body.lastname;
  const email = req.body.email;
  const affiliation = req.body.affiliation;
  const birthday = req.body.birthday;
  const interests = req.body.interests;
  if (interests.length < 2) {
    return res.send({
      success: false,
      msg: "You must have at least two interests."
    });
  } else if (username.length !== 0 && password.length !== 0 && firstname.length !== 0 && lastname.length !== 0 && email.length !== 0 && affiliation.length !== 0 && birthday.length !== 0 && interests && interests.length !== 0) { 
    const hashed = crypto.createHash("sha256").update(password).digest("hex");
    db.add_user(username, hashed, firstname, lastname, email, affiliation, birthday, interests, function(err, msg) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        return res.send({
          success: true,
          msg: null
        });
      }
    });
  } else {
    res.send({
      success: false,
      msg: "All input fields need to be completed!"
    });
  }
}

const changeEmail = function(req, res) {
  if (!req.session.username) {
    res.redirect("/");
    return;
  }
  db.update_email(req.session.username, req.body.newEmail, function(err, data) {
    if (err) {
      return res.send({
        success: false,
        msg: "Unsuccessful"
      });
    } else {
      db.log_last_action(req.session.username, function(err, data) {
        if (err) {
          console.error(JSON.stringify(err, null, 2));
        } else {
          res.send({
            success: true,
            msg: null
          });
        }
      });
    }
  });
}

const changePassword = function(req, res) {
  if (!req.session.username) {
    res.redirect("/");
    return;
  }
  const hashed = crypto.createHash("sha256").update(req.body.newPassword).digest("hex");
  db.update_password(req.session.username, hashed, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: "Unsuccessful"
      });
    } else {
      db.log_last_action(req.session.username, function(err, data) {
        if (err) {
          console.error(JSON.stringify(err, null, 2));
        } else {
          res.send({
            success: true,
            msg: null
          });
        }
      });
    }
  });
}

const changeAffiliation = function(req, res) {
  if (!req.session.username) {
    res.redirect("/");
    return;
  }
  db.update_affiliation(req.session.username, req.body.newAffiliation, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      const content = "I changed my affiliation to " + req.body.newAffiliation + "!";
      db.make_post(req.session.username, content, function(err, data) {
        if (err) {
          res.send({
            success: false,
            msg: JSON.stringify(err, null, 2)
          });
        } else {
          db.log_last_action(req.session.username, function(err, data) {
            if (err) {
              console.error(JSON.stringify(err, null, 2));
            } else {
              res.send({
                success: true,
                msg: null
              });
            }
          });
        }
      });
    }
  });
}

const changeInterests = function(req, res) {
  if (!req.session.username) {
    res.redirect("/");
    return;
  }
  db.update_interests(req.session.username, req.body.newInterests, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      var content = "I am now interested in ";
      for (var i = 0; i < req.body.newInterests.length - 1; ++i) {
        content += req.body.newInterests[i] + ", ";
      }
      content += "and " + req.body.newInterests[req.body.newInterests.length - 1] + "!";
      db.make_post(req.session.username, content, function(err, data) {
        if (err) {
          res.send({
            success: false,
            msg: JSON.stringify(err, null, 2)
          });
        } else {
          db.log_last_action(req.session.username, function(err, data) {
            if (err) {
              console.error(JSON.stringify(err, null, 2));
            } else {
              res.send({
                success: true,
                msg: null
              });
            }
          });
        }
      });
    }
  });
}

const searchScan = function(req, res) {
  if (!req.session.username) {
    res.redirect("/");
    return;
  }
  db.search_scan(req.body.text, function(err, data) {
    if (err) {
      res.send({
        success: false,
        msg: JSON.stringify(err, null, 2)
      });
    } else {
      res.send({
        success: true,
        data: data
      });
    }
  });
}

const signout = function(req, res) {
  if (req.session.username) {
    delete req.session.username;
  }
  res.redirect("/");
}

const addFriend = function(req, res) {
  if (req.session.username) {
    db.add_friend(req.session.username, req.body.friend, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        db.log_last_action(req.session.username, function(err, data) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          } else {
            res.send({
              success: true,
              msg: null
            });
          }
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const removeFriend = function(req, res) {
  if (req.session.username) {
    db.remove_friend(req.session.username, req.body.friend, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        db.log_last_action(req.session.username, function(err, data) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          } else {
            res.send({
              success: true,
              msg: null
            });
          }
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const makePost = function(req, res) {
  if (req.session.username) {
    db.make_post(req.session.username, req.body.content, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        db.log_last_action(req.session.username, function(err, data) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          } else {
            res.send({
              success: true,
              msg: null
            });
          }
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const makePostWall = function(req, res) {
  if (req.session.username) {
    db.make_post_to_wall(req.session.username, req.body.content, req.body.recipient, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        db.log_last_action(req.session.username, function(err, data) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          } else {
            res.send({
              success: true,
              msg: null
            });
          }
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const getPosts = function(req, res) {
  if (req.session.username) {
    db.get_friends(req.session.username, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        var docClient = new AWS.DynamoDB.DocumentClient();
        const promises = [];
        data.Items.forEach(function(item) {
          item.friends.SS.forEach(function(friend) {
            const params = {
              TableName: "posts",
              KeyConditionExpression: "author = :x",
              ExpressionAttributeValues: {
                ":x": friend
              }
            };
            promises.push(docClient.query(params).promise().then(
              function(data) {
                return data.Items;
              },
              function(err) {
                console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
              }
            ));
          });
        });
        Promise.all(promises).then(function(a) {
          const commentPromises = [];
          a.forEach(function(b) {
            b.forEach(function(data) {
              const key = data.author + "$" + data.timestamp;
              const commentParams = {
                TableName: "reactions",
                KeyConditionExpression: "authortime = :x",
                ExpressionAttributeValues: {
                  ":x": key
                }
              };
              commentPromises.push(docClient.query(commentParams).promise().then(
                function(x) {
                  data.comments = x.Items;
                  return data;
                },
                function(err) {
                  console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
                }
              ));
            });
          });
          Promise.all(commentPromises).then(function(output) {
            return res.send({
              success: true,
              data: output,
              msg: null
            });
          });
        },
        function(err) {
          return res.send({
            success: false,
            data: null,
            msg: JSON.stringify(err, null, 2)
          });
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const getPostsWall = function(req, res) {
  if (req.session.username) {
    db.get_friends(req.body.user, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        var docClient = new AWS.DynamoDB.DocumentClient();
        const promises = [];
        data.Items.forEach(function(item) {
          item.friends.SS.forEach(function(friend) {
            const params = {
              TableName: "posts",
              KeyConditionExpression: "author = :x",
              FilterExpression: "isWall = :y",
              ExpressionAttributeValues: {
                ":x": friend,
                ":y": req.body.user
              }
            };
            promises.push(docClient.query(params).promise().then(
              function(data) {
                return data.Items;
              },
              function(err) {
                console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
              }
            ));
          });
        });
        Promise.all(promises).then(function(a) {
          const commentPromises = [];
          a.forEach(function(b) {
            b.forEach(function(data) {
              const key = data.author + "$" + data.timestamp;
              const commentParams = {
                TableName: "reactions",
                KeyConditionExpression: "authortime = :x",
                ExpressionAttributeValues: {
                  ":x": key
                }
              };
              commentPromises.push(docClient.query(commentParams).promise().then(
                function(x) {
                  data.comments = x.Items;
                  return data;
                },
                function(err) {
                  console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
                }
              ));
            });
          });
          Promise.all(commentPromises).then(function(output) {
            return res.send({
              success: true,
              data: output,
              msg: null
            });
          });
        },
        function(err) {
          return res.send({
            success: false,
            data: null,
            msg: JSON.stringify(err, null, 2)
          });
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const getPostsByAuthor = function(req, res) {
  if (req.session.username) {
    db.get_posts_by_author(req.body.author, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        return res.send({
          success: true,
          data: data.Items
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const makeComment = function(req, res) {
  if (req.session.username) {
    db.make_comment(req.body.key, req.body.content, req.session.username, function(err, data) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        db.log_last_action(req.session.username, function(err, data) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          } else {
            res.send({
              success: true,
              msg: null
            });
          }
        });
      }
    });
  } else {
    res.redirect("/");
  }
}

const getComments = function(req, res) {
  if (req.session.username) {
    db.get_comments(req.body.authortime, function(err, x) {
      if (err) {
        return res.send({
          success: false,
          msg: JSON.stringify(err, null, 2)
        });
      } else {
        return res.send({
          success: true,
          data: x
        });
      }
    });
  } else {
    res.redirect("/");
  }
}



//refresh when leaving page
//deleting the only room
//initializing no rooms





//SOCKET IO ROUTES

const chat = function(req, res) {

  if(!req.session.username) {
    res.redirect('/');
  } else {
    res.render("chat.ejs", {});
  }
  
};

const io_on = function(socket) {

  if (!socket.handshake.session.username) {
    //res.redirect('/') //how to do this?
  } else {
  console.log('a user connected');
  console.log(socket.handshake.session);

  socket.emit('init', socket.handshake.session.username)
  console.log("init")

  var r;
  var invs;
  //get rooms
  db.login_lookup(socket.handshake.session.username, function(err, data) {
    console.log(data.Items[0].rooms.L);
    r = data.Items[0].rooms.L
    invs = data.Items[0].chatInvites.L;

    //Getting all messages on page load
    if(r[0] != null) {
      db.get_Messages(r[0].s, function(err,data) {
        if(err) {
          console.log(err)
        } else {
          //console.log(data);
          var send = []
    
          var moreData = {
            user : socket.handshake.session.username,
            rooms : r,
            invites: invs,
            currentRoom : r[0].S
          }
          send.push(data);
          send.push(moreData)
          //console.log(send)
          socket.emit('prev_messages', send);
    
          socket.emit('chat')
        }
    
        })

    } else if (invs[0] != null) {

      console.log(invs[0])
      var send = []
  
      var moreData = {
        user : socket.handshake.session.username,
        rooms : r,
        invites: invs,
        currentRoom : null
      }
      send.push(null);
      send.push(moreData)
      //console.log(send)
      socket.emit('prev_messages', send);

      socket.emit('chat')

    }
    

  })
  

  //Receiving new message
  socket.on("test", arg => {
    console.log(socket.handshake.session.username);
    db.addMessage(socket.handshake.session.username, arg.room, arg.message, function(err,data) {
      if(err) {
          console.log(err)
      } else {
          //console.log(data);
      }
    });
    console.log("message received");
    socket.emit('chat message', arg);

  });

  //Refreshing the page
  socket.on("refresh", arg => {
    console.log("Refreshing");

    db.get_Messages(arg, function(err,data) {
      if(err) {
         console.log(err)
      } else {
         //console.log(data);
         var send = []
   
         var username = {
           user : socket.handshake.session.username
         }
         send.push(data);
         send.push(username)
         //console.log(send)
         socket.emit('refr', send);
      }

      
   
      });
  });

  socket.on("change room", arg => {

    var r;
    var invs;
    db.login_lookup(socket.handshake.session.username, function(err, data) {
      console.log(data);
      r = data.Items[0].rooms.L
      invs = data.Items[0].chatInvites.L;

      db.get_Messages(arg, function(err,data) {
        if(err) {
           console.log(err)
        } else {
           console.log(data);
           var send = []

           console.log(arg)
     
           var moreData = {
             user : socket.handshake.session.username,
             rooms : r,
             invites : invs,
             currentRoom : arg
           }
           send.push(data);
           send.push(moreData)
           //console.log(send)
           socket.emit('prev_messages', send);
  
          }
        })
    })
  })


  socket.on("sendGroupInvite", arg => {
    db.check_friends(socket.handshake.session.username, function(err, da) {
      var t = da.Items[0].friends.SS;
      console.log(da.Items[0].friends.SS);
      console.log(arg.message)

      var u = t.includes(arg.message);

      if (u) {
        //check if the invite exists for the other person
        db.login_lookup(arg.message, function(err, d) {
          var invExists = false;
          if (d.Items[0].chatInvites != null) {
            invList = d.Items[0].chatInvites.L
            console.log(invList)
            console.log("yes")
            //check if our user is in their invitelist
            invList.forEach(x=>{
              console.log(x.S)
              console.log(arg.message)
              if(x.S == socket.handshake.session.username) {
                invExists = true;
                console.log(invExists)
              }
            })
          }   

          console.log(invExists)
          if(!invExists) {
            console.log("yesyes")
            //add invite for the recepient
            db.add_invite(arg.room, arg.message, function(err,dat){
              //console.log(dat.Items[0].chatInvites)
            })
          }
        })

      }
      
    })
  })


  socket.on("sendInvite", arg => {
    db.check_friends(socket.handshake.session.username, function(err, da) {
      var t = da.Items[0].friends.SS;
      console.log(da.Items[0].friends.SS);
      console.log(arg.message)

      var u = t.includes(arg.message);

      if (u) {
        //check if the invite exists for the other person
        db.login_lookup(arg.message, function(err, d) {
          var invExists = false;
          if (d.Items[0].chatInvites != null) {
            invList = d.Items[0].chatInvites.L
            console.log(invList)
            console.log("yes")
            //check if our user is in their invitelist
            invList.forEach(x=>{
              console.log(x.S)
              console.log(arg.message)
              if(x.S == socket.handshake.session.username) {
                invExists = true;
                console.log(invExists)
              }
            })
          }   

          console.log(invExists)
          if(!invExists) {
            console.log("yesyes")
            //add invite for the recepient
            db.add_invite(socket.handshake.session.username, arg.message, function(err,dat){
              //console.log(dat.Items[0].chatInvites)
            })
          }
        })

      }
      
    })
  })

  socket.on("deleteRoom", arg => {

    console.log(arg.message)

    console.log("delroom received")
    db.delete_room(socket.handshake.session.username, arg.message, function(err, data){
      
    })

  })

  socket.on("addRoom", arg => {

    console.log(arg);
    var o = arg.message.split(",")
    o.push(socket.handshake.session.username)
    o.sort();
    var p = o.join();


    console.log(p);

    console.log("Adding room " + arg)
    db.check_friends(socket.handshake.session.username, function(err, da) {

      var t = da.Items[0].friends.SS;
      console.log(da.Items[0].friends.SS);
      console.log(o)

      //check if all friends exist
      var u = o.every(x => t.includes(x));
      if (u) {
        //check if room exists
        db.login_lookup(socket.handshake.session.username, function(err, d) {
          var roomExists = false;
          var roomList = d.Items[0].rooms.L
          roomList.forEach(x=>{
            if(x.S == p) {
              roomExists = true;
            }
          })
          if(!roomExists) {
            //add for current user
            db.add_room(socket.handshake.session.username, p, function(err,dat){
              //add this for every other person
              o.forEach(x=>{
                if (x != socket.handshake.session.username) {
                  db.add_room(x, p, function(err,dat){
                    if (err) {
                      console.log(err);
                    }
                  })
                }
              })
              console.log(dat)
              db.login_lookup(socket.handshake.session.username, function(err, data) {
                console.log(data);
                var r = data.Items[0].rooms.L
                var invs = data.Items[0].rooms.L
        
                db.get_Messages(p, function(err,data) {
                  if(err) {
                    console.log(err)
                  } else {
                    console.log(data);
                    var send = []
              
                    var moreData = {
                      user : socket.handshake.session.username,
                      rooms : r,
                      invites : invs,
                      currentRoom : arg.room
                    }
                    send.push(data);
                    send.push(moreData)
                    //console.log(send)
                    socket.emit('prev_messages', send);
            
                    }
                  })
              })
            })

          }

        })
      } else {
        //include some error message
      }
    }) 
  }) 
  
  
  socket.on("deleteInvite", arg => {
    
    db.delete_invite(socket.handshake.session.username, arg.message, function(err, data){
      
    })

  })

}
}

// SETTINGS ROUTES

const getSettings = function(req, res) {
  if (req.session.username) {
    res.render("settings.ejs", {});
  } else {
    res.redirect("/");
  }
}

// NEWSFEED ROUTES

const getNewsFeed = function(req, res) {
  if (req.session.username) {
    res.render("newsfeed.ejs", {});
  } else {
    res.redirect("/");
  }
}

const getSearchNews = function(req, res) {
  if (req.session.username) {
    res.render("searchnews.ejs", {});
  } else {
    res.redirect("/");
  }
}

// SEARCH ROUTES

const getSearch = function(req, res) {
  if (req.session.username) {
    res.render("search.ejs", {});
  } else {
    res.redirect("/");
  }
}

// VISUALIZER ROUTES

const getVisualizer = function(req, res) {
  if (req.session.username) {
    res.render("friendvisualizer.ejs");
  } else {
    res.redirect("/");
  }
}

const initVisualization = function(req, res) {
  var json = {"id": req.session.username, "name": "", "children": [], "data": []};
  db.login_lookup(req.session.username, function(err, data) {
    if (err) {
      res.send({"id": req.session.username, "name": "", "children": [], "data": []});
    } else {
      json.name = data.Items[0].firstname.S;
      db.get_friends(req.session.username, function(err, friends) {
        if (err) {
          res.send(json);
        } else {
          var docClient = new AWS.DynamoDB.DocumentClient();
          const promises = [];
          friends = friends.Items[0].friends.SS;
          friends.forEach(function(friend) {
            if (friend === req.session.username) {
              return;
            }
            const fParams = {
              TableName: "users",
              KeyConditionExpression: "username = :x",
              ProjectionExpression: "firstname",
              ExpressionAttributeValues: {
                ":x": friend
              }
            };
            promises.push(docClient.query(fParams).promise().then(
              function(data) {
                return {username: friend, firstname: data.Items[0].firstname};
              },
              function(err) {
                console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
              }
            ))
          });
          Promise.all(promises).then(function(x) {
            x.forEach(function(name) {
              json.children.push({"id": name.username, "name": name.firstname, "children": [], "data": []});
            });
            res.send(json);
          });
        }
      });
    }
  });
}

const updateVisualization = function(req, res) {
  var json = {"id": req.params.user, "name": "", "children": [], "data": []};
  db.login_lookup(req.params.user, function(err, data) {
    if (err) {
      res.send(json);
    } else {
      json.name = data.Items[0].firstname.S;
      db.get_friends(req.params.user, function(err, friends) {
        if (err) {
          res.send(json);
        } else {
          var docClient = new AWS.DynamoDB.DocumentClient();
          const promises = [];
          friends = friends.Items[0].friends.SS;
          const checkPermsPromises = [];
          const checkFriends = {
            TableName: "friends",
            KeyConditionExpression: "username = :x",
            ProjectionExpression: "friends",
            ExpressionAttributeValues: {
              ":x": req.session.username
            }
          };
          const affiliationParams = {
            TableName: "users",
            KeyConditionExpression: "username = :x",
            ProjectionExpression: "affiliation",
            ExpressionAttributeValues: {
              ":x": req.session.username
            }
          };
          checkPermsPromises.push(docClient.query(checkFriends).promise().then(
            function(y) {
              return y.Items[0].friends.values;
            },
            function(err) {
              console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
            }
          ));
          checkPermsPromises.push(docClient.query(affiliationParams).promise().then(
            function(y) {
              return y.Items[0].affiliation;
            },
            function(err) {
              console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
            }
          ));
          Promise.all(checkPermsPromises).then(function(x) {
            const userPromises = [];
            friends.forEach(function(friend) {
              if (!x[0].includes(friend)) {
                const affParams = {
                  TableName: "users",
                  KeyConditionExpression: "username = :x",
                  ProjectionExpression: "username, affiliation",
                  ExpressionAttributeValues: {
                    ":x": friend
                  }
                };
                userPromises.push(docClient.query(affParams).promise().then(
                  function(y) {
                    return y.Items[0];
                  },
                  function(err) {
                    console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
                  }
                ));
              }
            });
            Promise.all(userPromises).then(function(z) {
              z.forEach(function(item) {
                if (item.affiliation !== x[1]) {
                  friends.splice(friends.indexOf(item.username), 1);
                }
              });
              friends.forEach(function(friend) {
                if (friend === req.params.user) {
                  return;
                }
                const fParams = {
                  TableName: "users",
                  KeyConditionExpression: "username = :x",
                  ProjectionExpression: "firstname",
                  ExpressionAttributeValues: {
                    ":x": friend
                  }
                };
                promises.push(docClient.query(fParams).promise().then(
                  function(y) {
                    return {username: friend, firstname: y.Items[0].firstname};
                  },
                  function(err) {
                    console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
                  }
                ))
              });
              Promise.all(promises).then(function(x) {
                x.forEach(function(name) {
                  json.children.push({"id": name.username, "name": name.firstname, "children": [], "data": []});
                });
                res.send(json);
              });
            });
          });
        }
      });
    }
  });
}

const routes = {
  get_login_page: renderLogin,
  get_user: getUser,
  get_friends: getFriends,
  check_login: checkLogin,
  get_signup_page: renderSignup,
  signup_user: signupUser,
  change_email: changeEmail,
  change_password: changePassword,
  change_affiliation: changeAffiliation,
  change_interests: changeInterests,
  get_feed: getFeed,
  sign_out: signout,
  chat : chat,
  make_post: makePost,
  make_post_wall: makePostWall,
  make_comment: makeComment,
  get_comments: getComments,
  get_posts: getPosts,
  get_posts_wall: getPostsWall,
  get_posts_by_author: getPostsByAuthor,
  render_wall: renderWall,
  io_on : io_on,
  get_settings: getSettings,
  get_newsfeed: getNewsFeed,
  get_searchnews: getSearchNews,
  get_search: getSearch,
  search_scan: searchScan,
  add_friend: addFriend,
  remove_friend: removeFriend,
  get_last_action: getLastAction,
  get_visualizer: getVisualizer,
  init_visualization: initVisualization,
  update_visualization: updateVisualization,
};

module.exports = routes;