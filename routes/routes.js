const fs = require("fs");
const crypto = require("crypto");
var db = require('../models/database.js');

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
          msg: JSON.stringify(msg)
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

const chat = function(req, res) {
  //if (!req.session.username) {
    //res.redirect("/")
  //}

  res.render("chat.ejs")

}

const makePost = function(req, res) {
  if (req.session.username) {
    db.make_post(req.body.content, req.session.username, function(err, data) {
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

const routes = {
  get_login_page: renderLogin,
  check_login: checkLogin,
  get_signup_page: renderSignup,
  signup_user: signupUser,
  get_feed: getFeed,
  sign_out: signout,
  chat : chat,
  make_post: makePost,
};

module.exports = routes;