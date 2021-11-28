const express = require('express');
const sessions = require('express-session');
const database = require('./models/database.js');
const routes = require('./routes/routes.js');
var app = express();

const http = require("http").createServer(app);
const io = require("socket.io")(http);

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
app.post("/getposts", routes.get_posts);
app.get("/wall", routes.render_wall);
io.on('connection', routes.io_on);




//Im doing the socket stuff here first because I havent figured out how to move it to routes im sorry christian 


/*io.on("connection", function(socket) {
   console.log('a user connected');
   db.getMessage(0, function(err,data) {
   if(err) {
      console.log(err)
   } else {
      console.log(data);
      socket.emit('chat message', data);
   }

   })
  
  socket.emit('chat message', "Hello");

  socket.on("test", arg => {
   console.log("message received")
   socket.emit('chat message', arg);
 });
});*/








console.log('Authors: Christian Sun, Belinda Xi, William Fan, Kishen Sivabalan');
http.listen(8080, () => console.log("HTTP server started on port 8080!"));




