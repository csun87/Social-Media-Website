const express = require('express');
const sessions = require('express-session');
const database = require('./models/database.js');
const routes = require('./routes/routes.js');
var app = express();

app.use(express.urlencoded());

var session = sessions({
   secret: 'ajxkciwjio2oSIFKcjSKWO*@#kdjC)Sk2jSkco',
   resave: false,
   saveUninitialized: true,
   cookie: {}
});
app.use(session);
app.use(function(req, res, next) {
   res.locals.username = req.session.username;
   next();
})

const socketSession = require("express-socket.io-session");
const http = require("http").createServer(app);
const io = require("socket.io")(http);

io.use(socketSession(session, {
   autoSave:true
}));


app.get("/", routes.get_login_page);
app.get("/login", routes.get_login_page);
app.get("/signup", routes.get_signup_page);
app.post("/createaccount", routes.signup_user);
app.post("/getuser", routes.get_user);
app.post("/checklogin", routes.check_login);
app.get("/feed", routes.get_feed);
app.get("/newsfeed", routes.get_newsfeed);
app.get("/searchnews", routes.get_searchnews);
app.get("/signout", routes.sign_out);
app.get("/chat", routes.chat);
app.post("/makepost", routes.make_post);
app.post("/getposts", routes.get_posts);
app.post("/getpostsbyauthor", routes.get_posts_by_author);
app.get("/wall", routes.render_wall);
app.post("/addfriend", routes.add_friend);
app.post("/makecomment", routes.make_comment);
app.post("/getcomments", routes.get_comments);
app.get("/settings", routes.get_settings);
app.post("/changeemail", routes.change_email);
app.post("/changepassword", routes.change_password);
app.post("/changeaffiliation", routes.change_affiliation);
app.post("/changeinterests", routes.change_interests);
app.get("/search", routes.get_search);
app.post("/searchscan", routes.search_scan);

http.listen(8080, function() {
   console.log("Welcome to PennBook, Team G13's NETS-212 final project!");
   console.log('Authors: Christian Sun (chsun), Kishen Sivabalan (kishens), Belinda Xi (belindax), & William Fan (willfan)');
   console.log("HTTP server started on port 8080! Go to http://localhost:8080\n\n");
});




