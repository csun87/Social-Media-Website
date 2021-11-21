const express = require('express');
const sessions = require('express-session');
const routes = require('./routes/routes.js');
//const socket = require('socket.io') (http)
var app = express();

app.use(express.urlencoded());
app.use(sessions({
   secret: 'ajxkciwjio2oSIFKcjSKWO*@#kdjC)Sk2jSkco',
   resave: false,
   saveUninitialized: true,
   cookie: {}
}));

app.get("/", routes.get_login_page);
app.get("/login", routes.get_login_page);
app.get("/signup", routes.get_signup_page);
app.post("/createaccount", routes.signup_user);
app.post("/checklogin", routes.check_login);
app.get("/feed", routes.get_feed);
app.get("/signout", routes.sign_out);
app.get("/chat", routes.chat);
app.post("/makepost", routes.make_post);

console.log('Authors: Christian Sun, Belinda Xi, William Fan, Kishen Sivabalan');
app.listen(8080, () => console.log("HTTP server started on port 8080!"));