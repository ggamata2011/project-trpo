'use strict';
//Requires for using Environment variables
require('dotenv').config();

//Instantiate App with express
var express = require('express');
var app = express();

//Instantiate OAuth client
// Will be moved onto environment variables in future instances
const OAuthClient= require('intuit-oauth')
const oauthClient = new OAuthClient({
  clientId: 'AB1oMdud4edbP0J8ZGuWxGFVSlBCYw6hQnMtMX8QnbZ0t7SCQY',
  clientSecret: 'Ki4b9gk5YUe0oNNKJqyRfDfWSfH4A14kZLwjkPqt',
  environment: 'sandbox' || 'production',
  redirectUri: 'http://localhost:3000/TA-Intuit-Callback',
});

//Body-parser used for parsing and organizing request data 
const bodyParser = require('body-parser');

//Instantiate Token to null 
let OAUTH2_Token = null;





//Send Hello World, will Change to Render initial page
app.get('/', function (req, res) {
  res.send('Hello World! this is a test, this should render a single button page');
});

//Initiate OAuthFlow, Sets scopes, sets authURI and redirects
app.get('/Initiate-OAuth',function (req,res ){

  //Instantiate AuthURI
  const authURI = oauthClient.authorizeUri({
    scope:[OAuthClient.scopes.Accounting,OAuthClient.scopes.Payment,OAuthClient.scopes.OpenId],
    state:'ta-intuit-test'
  });

  //Begin Redirect
  res.redirect(authURI);
});

//Callback Page
app.get('/TA-Intuit-Callback', function (req, res) {
  //res.send('Hello World! this is a test for the callback function');
  //Callback logic for storing shit goes into here
  oauthClient
  .createToken(req.url)
  .then(function (authResponse) {
    //console.log('The Token is  ' + JSON.stringify(authResponse.json));
    OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);
  })
  .catch(function (e) {
    console.error(e);
  });

  //console.log('The Token is  ' + OAUTH2_Token);

  //Send to webpage just to check if Token is working
  res.send(OAUTH2_Token);

  //Start 60 min timer interval
  // Schedule the token refresh to happen every 60 minutes (3600 seconds)
  setInterval(GetRefreshToken, 60 * 60 * 1000);
  
});

//Functions for Refresh Token, asynchronous, linked to SetInterval
 async function GetRefreshToken()
{

}

//Code below will contain Functions for starting API calls
function GetInvoiceData()
{

}

//Code Below are the Endpoints for getting Data


//Listen for request on logs to vs
app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
  console.log('http://localhost:3000')
});

//Functions to Organize Data into their Column Row Formats


//Code Below is saved for Google Big Query API