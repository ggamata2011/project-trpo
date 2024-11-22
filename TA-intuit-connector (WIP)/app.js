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
  clientId: process.env.CLIENT_ID,
  clientSecret: process.env.CLIENT_SECRET,
  environment: 'sandbox' || 'production',
  redirectUri: process.env.CLIENT_REDIRECT,
});

//Body-parser used for parsing and organizing request data 
const bodyParser = require('body-parser');
app.use(bodyParser.json());

//Instantiate Token to null 
let OAUTH2_Token = null;

//Initiate OAuthFlow, Sets scopes, sets authURI and redirects
app.get('/Initiate-OAuth',function (req,res ){

     //Instantiate AuthURI
  const authURI = oauthClient.authorizeUri({
    scope:[OAuthClient.scopes.Accounting],
    state:'ta-intuit-test'
  });

  //Begin Redirect
  res.redirect(authURI);
});

//Callback Page after OAuth-Redirec 
app.get('/TA-Intuit-Callback', function (req, res) {
  //res.send('Hello World! this is a test for the callback function');
  //Callback logic for storing shit goes into here
 
  oauthClient
  .createToken(req.url)
  .then(function (authResponse) {
    console.log('The Token is  ' + JSON.stringify(authResponse.json));
    OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);

    //Get Invoice (Temporarily commented out)
    //GetInvoiceData();

    // Schedule the token refresh to happen every 60 minutes (3600 seconds)
    // Old CODE 60 * 60 * 1000
    setInterval(RefreshAccessToken, 55 * 60 * 1000);
    console.log('Refresh Access Token Timer Started')

    //Set Interval for Getting Invoice Data
    setInterval(GetInvoiceData,60 * 60 * 1000);

  })
  .catch(function (e) {
    console.error(e);
  });

});

//API endpoint for Refreshing access token (mainly for testing)
app.get('/refreshAccessToken', function (req, res) {
  oauthClient
    .refresh()
    .then(function (authResponse) {
      console.log(`\n The Refresh Token is  ${JSON.stringify(authResponse.json)}`);
      OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);
      res.send(OAUTH2_Token);
    })
    .catch(function (e) {
      console.error(e);
    });
});

app.get('/invoice', function(req,res ){
  //Instantiate Invoice data
  let Invoicedata = null;

  if(CheckAccessToken())
  {
    const companyID = oauthClient.getToken().realmId;

    const url =
      oauthClient.environment == 'sandbox'
        ? OAuthClient.environment.sandbox
        : OAuthClient.environment.production;

    oauthClient
    .makeApiCall({ url: `${url}v3/company/${companyID}/query?query=select * from Invoice&minorversion=73`})
    .then(function (response) {
    console.log(`\n The response for API call is :${JSON.stringify(response.json)}`);
    
    })
    .catch(function (e) {
    console.error(e);
    });
  }

})

//Check if Token is still valid, may call Refresh Token if needed
function CheckAccessToken()
{
   if(oauthClient.isAccessTokenValid())
   {
    console.log('access token is good');
   }
   else
   {
    RefreshAccessToken();
    console.log('access token is no good');
   }

   return oauthClient.isAccessTokenValid();

}

//Refreshes access token
function RefreshAccessToken()
{
  oauthClient
    .refresh()
    .then(function (authResponse) {
      console.log(`\n The Refresh Token is  ${JSON.stringify(authResponse.json)}`);
      OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);
    })
    .catch(function (e) {
      console.error(e);
    });
}

//Code below will contain Functions for starting API calls
function GetInvoiceData()
{
   //Instantiate Invoice data
   let Invoicedata = null;

    if(CheckAccessToken())
    {
      const companyID = oauthClient.getToken().realmId;

      const url =
        oauthClient.environment == 'sandbox'
          ? OAuthClient.environment.sandbox
          : OAuthClient.environment.production;

      oauthClient
      .makeApiCall({ url: `${url}v3/company/${companyID}/query?query=select * from Invoice&minorversion=73`})
      .then(function (response) {
      console.log(`\n The response for API call is :${JSON.stringify(response.json)}`);
      })
      .catch(function (e) {
      console.error(e);
      });

      //If we have invoice data, parse data into fields
      //or Ingest Data onto BQ API
      if(Invoicedata)
      {

      }
}
else
{
  console.log("Bad Access Token!");
}

}

let PORT = 3000;
//Listen for request on logs to vs
app.listen(PORT, function () {
  console.log(`Example app listening on port ${PORT}!`);
  console.log(`http://localhost:${PORT}`)
  console.log(`To Start O-Auth Process click on link below`)
  console.log(`http://localhost:${PORT}/Initiate-OAuth`)
});

//Functions to Organize Data into their Column Row Formats
//Code Below is saved for Google Big Query API