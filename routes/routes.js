const fs = require("fs");
const crypto = require("crypto");
var db = require('../models/database.js');
const AWS = require("aws-sdk");
const { brotliCompressSync } = require("zlib");

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
          res.render("wall_friend.ejs", {});
        } else {
          res.render("wall_stranger.ejs", {});
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
          res.send({
            success: true,
            msg: null
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
    res.render("main.ejs", {});
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
      return res.send({
        success: true,
        msg: null
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
      res.send({
        success: true,
        msg: null
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
        msg: "Unsuccessful"
      });
    } else {
      res.send({
        success: true,
        msg: null
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
        msg: "Unsuccessful"
      });
    } else {
      res.send({
        success: true,
        msg: null
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
        msg: "Unsuccessful"
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
        return res.send({
          success: true,
          msg: null
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
        return res.send({
          success: true,
          msg: null
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
        return res.send({
          success: true,
          msg: null
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


//SOCKET IO ROUTES

const chat = function(req, res) {

  if(!req.session.username) {
    res.redirect('/');
  } else {
    res.render("chat.ejs", {});
  }
  
};

const io_on = function(socket) {
  console.log('a user connected');

  console.log(socket.handshake.session);

  var r;
  //get rooms
  db.login_lookup(socket.handshake.session.username, function(err, data) {
    console.log(data.Items[0].rooms.L);
    r = data.Items[0].rooms.L

    //Getting all messages on page load
    db.get_Messages(r[0].s, function(err,data) {
      if(err) {
        console.log(err)
      } else {
        //console.log(data);
        var send = []
  
        var moreData = {
          user : socket.handshake.session.username,
          rooms : r,
          currentRoom : r[0].S
        }
        send.push(data);
        send.push(moreData)
        //console.log(send)
        socket.emit('prev_messages', send);
  
        socket.emit('chat')
      }
  
      })

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
    db.login_lookup(socket.handshake.session.username, function(err, data) {
      console.log(data);
      r = data.Items[0].rooms.L

      db.get_Messages(arg, function(err,data) {
        if(err) {
           console.log(err)
        } else {
           console.log(data);
           var send = []
     
           var moreData = {
             user : socket.handshake.session.username,
             rooms : r,
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

  //sort room id to make sure unique
  //sort messages
  //front end + front end

  socket.on("addRoom", arg => {

    console.log(arg);
    var o = arg.message.split(",");
    console.log(o);

    console.log("Adding room " + arg)
/*
    var r;

    db.add_room(socket.handshake.session.username, arg.message, function(err,dat){
      console.log(dat)

      db.login_lookup(socket.handshake.session.username, function(err, data) {
        console.log(data);
        r = data.Items[0].rooms.L

        db.get_Messages(arg, function(err,data) {
          if(err) {
            console.log(err)
          } else {
            console.log(data);
            var send = []
      
            var moreData = {
              user : socket.handshake.session.username,
              rooms : r,
              currentRoom : arg
            }
            send.push(data);
            send.push(moreData)
            //console.log(send)
            socket.emit('prev_messages', send);
    
            }
          })
      })
    }*/
  })  

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

const routes = {
  get_login_page: renderLogin,
  get_user: getUser,
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
  make_comment: makeComment,
  get_comments: getComments,
  get_posts: getPosts,
  get_posts_by_author: getPostsByAuthor,
  render_wall: renderWall,
  io_on : io_on,
  get_settings: getSettings,
  get_newsfeed: getNewsFeed,
  get_searchnews: getSearchNews,
  get_search: getSearch,
  search_scan: searchScan,
  add_friend: addFriend
};

module.exports = routes;