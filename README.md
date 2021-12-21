Group: G13

Group Members (Name & PennKey):

	Christian Sun (chsun)
	Kishen Sivabalan (kishens)
	William Fan (willfan)
	Belinda Xi (belindax)



Implemented Features 

	All required features in the writeup have been implemented



Extra Credit Tasks

	N/A



Source Files

	app.js
	
	package.json
	
	package-lock.json
	
	pom.xml
	
	models/database.js
	
	routes/routes.js
	
	views/signup.ejs
	
	views/login.ejs
	
	views/chat.ejs
	
	views/friendvisualizer.ejs
	
	views/main.ejs
	
	views/newsfeed.ejs
	
	views/search.ejs
	
	views/searchnews.ejs
	
	views/settings.ejs
	
	views/signup.ejs
	
	views/wall_friend.ejs
	
	views/wall_stranger.ejs
	
	views/wall_self.ejs
	
	src/main/java/edu/upenn/cis/nets212/hw3/livy/ComputeRanksLivy.java
	
	src/main/java/edu/upenn/cis/nets212/hw3/livy/MyPair.java
	
	src/main/java/edu/upenn/cis/nets212/hw3/livy/SocialRankJob.java
	
	target/nets212-hw3-0.0.1-SNAPSHOT.jar



Declaration that code submitted was written by us

	We (Christian Sun, Kishen Sivabalan,  William Fan and Belinda Xi), declare that all code submitted within this project was written by us (excluding code given to us from previous HW files)


Instructions

	1. Establish DynamoDB tables 
		1a. users: username (String, primary key)
		1b. searchNews: keyword (String, primary key), url (String, sort key)
		1c. reactions: authortime (String, primary key), timestamp (number, sort key)
		1d. posts: author (String, primary key), timestamp (number, sort key)
		1e. newslikes: userid (String, primary key), url (String, primary key)
		1f.news: url (String, primary key)
		1g. messages2: groupid (String, primary key), timestamp (String, sort key)
		1h. friends: username (String, primary key)
	2. Run “npm install” inside of the root project directory
	3. Launch app.js (node app.js)
	4. Sign up to be a new user
	5. Login in with username and password
	6. Explore features of PennBook including, but not limited to: adding friends, posting on walls, changing account details, etc
	7. Chat instructions: 
		7a. Input a friend’s username in the “Add a new chat room” bar to send them a chat invite.
		7b. Once in a room, input another friend’s name in the “Invite someone to this chat” bar to add someone to the existing chat, which creates a new group chat
		7c. To leave a chat, type “Leave” in the input field next to the respective chat room button



