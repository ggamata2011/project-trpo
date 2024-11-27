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

//Big query API requires
const {BigQuery} = require('@google-cloud/bigquery');

const BQ = new BigQuery({
  keyFilename: 'TAAPI.json',
  projectId: 'ta-test-442511', // Replace with your project ID
})

//Schemas
const InvoiceSchema = [
  { name: 'TxnDate', type: 'STRING', mode: 'NULLABLE' },
  { name: 'TotalAmt', type: 'FLOAT', mode: 'NULLABLE' },
  { name: 'DocNumber', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CustomerName', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CustomerValue', type: 'STRING', mode: 'NULLABLE' },
  { name: 'Lines', type: 'STRING', mode: 'REPEATED' },
  { name: 'DueDate', type: 'STRING', mode: 'NULLABLE' },
  { name: 'Email', type: 'STRING', mode: 'NULLABLE' },
  { name: 'ShipAddr', type: 'STRING', mode: 'NULLABLE' },
  { name: 'BillAddr', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CreateTime', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'LastUpdatedTime', type: 'TIMESTAMP', mode: 'NULLABLE' },
];


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

    //Create Tables
    createTable("Invoice","Invoice_Detail");
    

    //Get Invoice (Temporarily commented out)
    GetInvoiceData();

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
async function GetInvoiceData()
{
   //Instantiate Invoice data
   let Invoicedata = null;
   let TransformedData = null;

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
      //console.log(`\n The response for API call is :${JSON.stringify(response.json)}`);

       console.log("\n\nOriginal Data:" + response.json + "\n\n");

        TransformedData = ConvertJSON(response.json);

        PushData("Invoice","Invoice_Detail",TransformedData);
      })
      .catch(function (e) {
      console.error(e);
      });

      //If we have invoice data, parse data into fields
      //or Ingest Data onto BQ API
      //if(Invoicedata)
      //{
        
      //}
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

//Push Data onto Google Big Query
async function PushData(DataID,TabID,RowData)
{
  
  try{
    
    const options = {
      autodetect: true,
      writeDisposition: 'WRITE_APPEND', // Appends data to the table
    };

     // Load data directly from memory
     const [job] = await BQ.dataset(DataID).table(TabID).insert(RowData);

     console.log(`Data loaded into BigQuery. Job: ${job.id}`);

  } catch  (error)
  {
    console.error('Error inserting');
    console.error('Error message:', error.message); // Log the error message
    console.error('Error details:', error); // Log the full error object for debugging
  }
    
}

//Convert to JSON new line delimited
function ConvertJSON(Data)
{
  //Wrap JSON data in array to use .map
  //Data = [Data];

  //return map
  //console.log(Data.map(item => JSON.stringify(item)))
  //return Data.map(item => JSON.stringify(item)).join('\n');
  // Assume `apiResponse.QueryResponse.Invoice` contains an array of invoices
  const invoices = Data.QueryResponse.Invoice || [];

  return invoices.map(invoice => {
    return {
      TxnDate: invoice.TxnDate || null,
      TotalAmt: invoice.TotalAmt || null,
      DocNumber: invoice.DocNumber || null,
      CustomerName: invoice.CustomerRef?.name || null,
      CustomerValue: invoice.CustomerRef?.value || null,
      Lines: invoice.Line ? JSON.stringify(invoice.Line) : null,
      DueDate: invoice.DueDate || null,
      Email: invoice.BillEmail?.Address || null,
      ShipAddr: invoice.ShipAddr ? JSON.stringify(invoice.ShipAddr) : null,
      BillAddr: invoice.BillAddr ? JSON.stringify(invoice.BillAddr) : null,
      CreateTime: invoice.MetaData?.CreateTime || null,
      LastUpdatedTime: invoice.MetaData?.LastUpdatedTime || null,
    };
  });
  
}

//Creates Table function (General)
async function createTable(DataID, TabID, schemaprofile) {
  const datasetId = DataID;
  const tableId = TabID;

  try {
    // Check if the dataset exists
    const [datasets] = await BQ.getDatasets();
    const datasetExists = datasets.some(dataset => dataset.id === datasetId);

    if (!datasetExists) {
      // Create a new dataset if it doesn't exist
      await BQ.createDataset(datasetId, { location: 'US' });
      console.log(`Dataset ${datasetId} created.`);
    } else {
      console.log(`Dataset ${datasetId} already exists.`);
    }

    // Reference the dataset
    const dataset = BQ.dataset(datasetId);

    // Check if the table exists
    const [tables] = await dataset.getTables();
    const tableExists = tables.some(table => table.id === tableId);

    if (!tableExists) {
      // Create the table if it doesn't exist
      const options = {
        schema: schemaprofile,
        location: 'US',
      };
      const [table] = await dataset.createTable(tableId, options);
      console.log(`Table ${table.id} created.`);
    } else {
      console.log(`Table ${tableId} already exists.`);
    }
  } catch (err) {
    console.error('Error creating or checking table:', err);
  }
}




