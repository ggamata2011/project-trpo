'use strict';
//Requires for using Environment variables
require('dotenv').config();




//Instantiate App with express
var express = require('express');
var app = express();

//Instantiate Intuit Client
const OAuthClient= require('intuit-oauth')
const oauthClient = new OAuthClient({
  clientId: process.env.CLIENT_ID,
  clientSecret: process.env.CLIENT_SECRET,
  environment: 'sandbox' || 'production',
  redirectUri: process.env.CLIENT_REDIRECT,
});

//some old code to omit   environment: 'sandbox' || 'production',


//Body-parser used for parsing and organizing request data 
const bodyParser = require('body-parser');
app.use(bodyParser.json());

//Parse JSON bodies
app.use(express.json());

//Big query API requires
const {BigQuery} = require('@google-cloud/bigquery');

const BQ = new BigQuery({
  keyFilename: 'tableu-442921-272d860b3fc9.json',
  projectId: 'tableu-442921', // Replace with your project ID
})

//Schemas
const InvoiceSchema = [
  { name: 'TxnDate', type: 'STRING', mode: 'NULLABLE' },
  { name: 'TotalAmt', type: 'FLOAT', mode: 'NULLABLE' },
  { name: 'DocNumber', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CustomerName', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CustomerValue', type: 'STRING', mode: 'NULLABLE' },
  { name: 'Lines', type: 'STRING', mode: 'NULLABLE' },
  { name: 'DueDate', type: 'STRING', mode: 'NULLABLE' },
  { name: 'Email', type: 'STRING', mode: 'NULLABLE' },
  { name: 'ShipAddr', type: 'STRING', mode: 'NULLABLE' },
  { name: 'BillAddr', type: 'STRING', mode: 'NULLABLE' },
  { name: 'CreateTime', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'LastUpdatedTime', type: 'TIMESTAMP', mode: 'NULLABLE' },
];


//Instantiate Token to null 
let OAUTH2_Token = null;


//Webhook API Endpoint
app.post('/TA-Intuit',(req,res) => {

 console.log('Received Request:' + req.body);
 //Some Logic to pull Invoice data or possibly a switch for other types of data
 GetInvoiceData();
 // Send a response
 res.status(200).send('Webhook received successfully');
   
});

app.get('/Start',async(req, res) => {
   //Check Token and IDs
   console.log("The Client Secret Is: " + process.env.CLIENT_SECRET);
   console.log("RealmID is: " + process.env.REALM_ID);
   console.log("Client Redirect is: " + process.env.CLIENT_REDIRECT);  
   console.log("The refresh token is " + process.env.REFRESH_TOKEN );

   let Data = await GetInvoiceData();
   res.send("Test Page Send for Refresh Token Accessed, displaying data\n\n" + JSON.stringify(Data));

   //push Data to big query
   PushInvoiceData(Data);
});
   

/* 
******************************************************************
*/
//Below 3 functions are for OAuth Purposes for obtaining Tokens, may not be used anymore in favor of refresh tokens
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
 
  console.log("Callback begun");
   InitateAccessTokenGET(req);
});
async function InitateAccessTokenGET(req)
{
  console.log('initate access token get');
  await oauthClient
  .createToken(req.url)
  .then(function (authResponse) {
    console.log('The Token is  ' + JSON.stringify(authResponse.json));
    OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);
  })
  .catch(function (e) {
    console.error(e);
  });

   //Create Tables
   await createTableBQ("Invoice","Invoice_Detail",InvoiceSchema);
    
   //Get Invoice (Temporarily commented out)
   await GetInvoiceData();

   // Schedule the token refresh to happen every 60 minutes (3600 seconds)
   // Old CODE 60 * 60 * 1000
   setInterval(RefreshAccessToken, 55 * 60 * 1000);
   console.log('Refresh Access Token Timer Started')

   //Set Interval for Getting Invoice Data
   setInterval(GetInvoiceData,60 * 60 * 1000);


}
//3 functions for OAUTH end here 

/* 
******************************************************************
*/

//Check if Token is still valid, may call Refresh Token if needed
async function CheckAccessToken()
{
   if( oauthClient.isAccessTokenValid())
   {
    console.log('access token is good, can fetch data');
   }
   else
   {
    console.log('access token invalid, refreshing token..');
    await RefreshAccessToken(2);
   }

   return oauthClient.isAccessTokenValid();

}

//Refreshes access token with current set OAUTH Token or supplied env
async function RefreshAccessToken(opt)
{
  
  switch(opt){

    //Case 1 used for refresh, if OAuth used
    case 1:
      await oauthClient
    .refresh()
    .then(function (authResponse) {
      console.log(`\n The Refreshed Access Token is  ${JSON.stringify(authResponse.json)}`);
      OAUTH2_Token = JSON.stringify(authResponse.json, null, 2);
    })
    .catch(function (e) {
      console.error(e);
    });
    break;

    //Case 2 used for refresh if refresh token is already provided in environment variable
    case 2:
      await oauthClient
  .refreshUsingToken(process.env.REFRESH_TOKEN)
  .then(function (authResponse) {
    console.log('Tokens refreshed : ' + JSON.stringify(authResponse.json));



  })
  .catch(function (e) {
      console.error('Request failed with status code:', e.response?.status);
      console.error('Response body:', e.response?.data);
      console.error('Headers:', e.response?.headers);
      console.error('Original error message:', e.message);
      console.error('Intuit Transaction ID:', e.intuit_tid || 'N/A');
  });

    break; 
}

   
    
  }
  
//Code below will contain Functions for starting API calls
async function GetInvoiceData()
{
   //Instantiate Invoice data
   let Invoicedata = null;

    if(await CheckAccessToken())
    {
      let companyID = '';
      
      if(process.env.REALM_ID != '')
      {
         companyID = process.env.REALM_ID;
      }
      else
      {
         companyID = oauthClient.getToken().realmId;
      }
      

      const url =
        oauthClient.environment == 'sandbox'
          ? OAuthClient.environment.sandbox
          : OAuthClient.environment.production;

      await oauthClient
      .makeApiCall({ url: `${url}v3/company/${companyID}/query?query=select * from Invoice&minorversion=73`})
      .then(function (response) {
      //console.log(`\n The response for API call is :${JSON.stringify(response.json)}`);
        Invoicedata = response.json;
      })
      .catch(function (e) {
      console.error(e);
      });

      return Invoicedata;        
}
else
{
  console.log("Bad Access Token!, unable to access data");
  return null
}

}

function PushInvoiceData(InvoiceData)
{
  if(InvoiceData)
  {
    let TransformedData = TransformJSON(InvoiceData);
    PushDataBQ("Invoice","Invoice_Detail",TransformedData);
  }
  else
  {
    console.log("Data not recieved, Data will not be pushed");
  }
  
}

//Convert to JSON new line delimited for Google BQ(used for streaming data inserts)
async function TransformJSON(Data)
{
  // Assume `apiResponse.QueryResponse.Invoice` contains an array of invoices
  if(Data)
  {
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
  else{
    console.log("No data in response");
  }
  
}

//Google Big Query Functions Below

//Push Data onto Google Big Query
async function PushDataBQ(DataID,TabID,RowData)
{
  try{
    const options = {
      autodetect: true,
      writeDisposition: 'WRITE_APPEND', // Appends data to the table
    };

     // Load data directly from memory
     const [job] = await BQ.dataset(DataID).table(TabID).insert(RowData,options);

     console.log(`Data loaded into BigQuery. Job: ${job.id}`);

  } catch  (error)
  {
    console.error('Error inserting');
    if (error.errors) {
      error.errors.forEach(err => {
        console.error('Row-level error:', JSON.stringify(err));
      });
    }

  }
    
}

//Creates BQ Table function (General)
async function createTableBQ(DataID, TabID, schemaprofile) {
  const datasetId = DataID;
  const tableId = TabID;

  //console.log("Schema Profile:" + schemaprofile);

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


let PORT = 3000;
//Listen for request on logs to vs
app.listen(PORT, function () {
  console.log(`Example app listening on port ${PORT}!`);
  console.log(`http://localhost:${PORT}`);
  console.log(`To Start O-Auth Process click on link below`);
  console.log(`http://localhost:${PORT}/Initiate-OAuth`);
  console.log('To Start process without OAuth using refresh tokens click on link below');
  console.log(`http://localhost:${PORT}/Start`);
});






