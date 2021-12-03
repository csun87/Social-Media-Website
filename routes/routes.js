const fs = require("fs");
const crypto = require("crypto");
var db = require('../models/database.js');
const AWS = require("aws-sdk");

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
  if (req.session.username) {
    res.render("wall.ejs", {});
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
  if (username.length !== 0 && password.length !== 0 && firstname.length !== 0 && lastname.length !== 0 && email.length !== 0 && affiliation.length !== 0 && birthday.length !== 0 && interests && interests.length !== 0) { 
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

const signout = function(req, res) {
  if (req.session.username) {
    delete req.session.username;
  }
  res.redirect("/");
}

const makePost = function(req, res) {
  if (req.session.username) {
    db.make_post(req.session.username, req.body.content, function(err, data) {
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
        const temp = {
          TableName: "posts",
          KeyConditionExpression: "author = :x",
          ExpressionAttributeValues: {
            ":x": req.session.username
          }
        };
        promises.push(docClient.query(temp).promise().then(
          function(data) {
            return data.Items;
          },
          function(err) {
            console.error("Unable to query. Error: ", JSON.stringify(err, null, 2));
          }
        ));
        data.Items.forEach(function(item) {
          const params = {
            TableName: "posts",
            KeyConditionExpression: "author = :x",
            ExpressionAttributeValues: {
              ":x": item.user2.S
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
        Promise.all(promises).then(function(data) {
          const posts = [];
          data.forEach(function(data) {
            posts.push(data);
          });
          return res.send({
            success: true,
            data: posts,
            msg: null
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

  //Getting all messages on page load
  db.get_Messages(0, function(err,data) {
   if(err) {
      console.log(err)
   } else {
      //console.log(data);
      var send = []

      var moreData = {
        user : socket.handshake.session.username,
        rooms : [0,1,7],
        currentRoom : 0
      }
      send.push(data);
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
    db.get_Messages(arg, function(err,data) {
      if(err) {
         console.log(err)
      } else {
         //console.log(data);
         var send = []
   
         var moreData = {
           user : socket.handshake.session.username,
           rooms : [0,1,7],
           currentRoom : arg
         }
         send.push(data);
         send.push(moreData)
         //console.log(send)
         socket.emit('prev_messages', send);

        }
      })
    

  })

}

const routes = {
  get_login_page: renderLogin,
  get_user: getUser,
  check_login: checkLogin,
  get_signup_page: renderSignup,
  signup_user: signupUser,
  get_feed: getFeed,
  sign_out: signout,
  chat : chat,
  make_post: makePost,
  make_comment: makeComment,
  get_comments: getComments,
  get_posts: getPosts,
  get_posts_by_author: getPostsByAuthor,
  render_wall: renderWall,
  io_on : io_on
};

module.exports = routes;