'use strict';
//Requires for using Environment variables
require('dotenv').config();
const { Buffer } = require('buffer');
const { Readable } = require('stream');
const fs = require('fs'); // For Node.js

const { BigQueryWriteClient } = require('@google-cloud/bigquery-storage');

//Big query API requires
const {BigQuery} = require('@google-cloud/bigquery');
//Instantiate App with express
var express = require('express');
var app = express();

//Body-parser used for parsing and organizing request data 
const bodyParser = require('body-parser');
app.use(bodyParser.json());

//Parse JSON bodies
app.use(express.json());

//Instantiate Intuit Client
const OAuthClient= require('intuit-oauth')
const oauthClient = new OAuthClient({
  clientId: process.env.CLIENT_ID,
  clientSecret: process.env.CLIENT_SECRET,
  environment: 'production',
  redirectUri: process.env.CLIENT_REDIRECT,

});

//some old code to environment: 'sandbox' || 'production',
const authURI = oauthClient.authorizeUri({
  scope:[OAuthClient.scopes.Accounting],
  state:'ta-intuit-test'
});

//CompanyID
const companyID = process.env.REALM_ID != '' ? process.env.REALM_ID : oauthClient.getToken().realmId;
//Check oauthEnvironment
const url = oauthClient.environment == 'sandbox' ? OAuthClient.environment.sandbox : OAuthClient.environment.production;

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

//Schema for Storing Refresh_Token
const TokenSchema = [{name:'Refresh_Token', type:'STRING', mode:'NULLABLE'}];

//Instantiate Token to null 
let OAUTH2_Token = null;

//Misc Dataset names, interchangeble
let TokenDataSet = 'IntuitKeys2';
let ProfitLossDataSetName = 'ProfitLoss2';


//Infer A Schema for a report
async function InferSchemaReport(data)
{
  let InferredSchema = [];

  //Get Column Names and Data Types
  for(let i = 0; i < data.Columns.Column.length; i++)
  {
    InferredSchema.push({ name: removeSpecialCharacters(data.Columns.Column[i].ColTitle), type: ResolveBQType(data.Columns.Column[i].ColType), mode: 'NULLABLE' });
  }

  return InferredSchema;
}

function removeSpecialCharacters(input) {
  // Replace all non-alphanumeric characters with a blank space
  return input.replace(/[^a-zA-Z0-9 ]/g, '');
}

//Resolves Type to their appropriate BQ Type
function ResolveBQType(type)
{
  //May add more as we come across more data
  switch(type)
  {
    case 'String':
      return 'STRING';
      break;
    case 'Date':
      return 'DATE';
      break;
    case 'Money':
      return "FLOAT";
      break;
    default:
      return 'STRING';
      break;
  }
}

//Used to create Separate BQ Tables for Reports
async function InferData(data,dataschema)
{
  // Holds Table names
   let InferredTableNames = [];
   //Tree Stack to keep of order
   let TreeStack = [];
   //Second Stack to keep of order
   let MemoryStack = [];

   //Get Top Level Column Names and Data Types
  for(let i = 0; i < data.Rows.Row.length; i++)
  {
    //Check if Header Exists
    if(data.Rows.Row[i].Header != undefined)
    {
      console.log('Root Node: ' + data.Rows.Row[i].Header.ColData[0].value);
      
      //Push Tree from first Level into stack
      TreeStack.push(data.Rows.Row[i]);

      //Traverse Parent Node using Stack
      while(TreeStack.length > 0)
      {
        //Set current level
         let CurrentLevel = TreeStack.pop();

         //If there are children, add to stack and go one level down
         // this iswhere im having issues
         if(CurrentLevel.Rows != undefined && Array.isArray(CurrentLevel.Rows.Row))
         {
          //Add Children one level down
          for(let i = 0; i < CurrentLevel.Rows.Row.length; i++)
          {    
              if(CurrentLevel.Rows.Row[i].Header != undefined)   
              {
                MemoryStack.push(CurrentLevel.Rows.Row[i]);
              }
              
          }   
         }  

         //Check if Current Level is at bottom
         if (CurrentLevel.Rows.Row[0].ColData != undefined && CurrentLevel.Rows.Row[0].type == 'Data')
          {
            //console.log("Table Name Data:" + CurrentLevel.Header.ColData[0].value);
            InferredTableNames.includes(CurrentLevel.Header.ColData[0].value) ? null:InferredTableNames.push(removeSpecialCharacters(CurrentLevel.Header.ColData[0].value));
            
            //Create Table
            let FullTable = [];

            //Loop through Each data fields to transform to appropriate schema
            for(let j = 0;j < CurrentLevel.Rows.Row.length; j++)
            {
              let TableRow = {};
              
              //loop through each subportion
              for(let k = 0; k < CurrentLevel.Rows.Row[j].ColData.length; k++)
              {
                //console.log('value item: ' + CurrentLevel.Rows.Row[j].ColData[k].value);

                //Some tables have an "amount" object with no value, must check for that
                if(dataschema[k].name == 'Amount' && CurrentLevel.Rows.Row[j].ColData[k].value == undefined || CurrentLevel.Rows.Row[j].ColData[k].value == "")
                {
                  TableRow[dataschema[k].name] = null;
                }
                else
                {
                  TableRow[dataschema[k].name] = CurrentLevel.Rows.Row[j].ColData[k].value;
                }
                
              }

              FullTable.push(TableRow);
            }

            //Create Dataset and Table, push contents to BigQuery
            await createTableBQ(ProfitLossDataSetName, removeSpecialCharacters(CurrentLevel.Header.ColData[0].value), dataschema);
            await PushDataBQ(ProfitLossDataSetName,removeSpecialCharacters(CurrentLevel.Header.ColData[0].value),FullTable);

          }


         //If Exhausted all children, push back onto MainStack
         if(TreeStack.length == 0 && MemoryStack.length != 0)
         {
           while(MemoryStack.length > 0)
           {
              TreeStack.push(MemoryStack.pop());
           }
         }        
      }
    }      
  }

  //Display Inferred TableNames
  console.log(InferredTableNames);
  
  return InferredTableNames;
}

app.get('/CreateTokenStore',async (req,res) => {
  await createTokenTable();
});

app.get('/PushTokenStore',async (req,res) => {
  await PushTokenBQ(process.env.REFRESH_TOKEN);
});

app.get('/Store-Keys',async (req,res) => {
  await createTokenTable();
  await new Promise(resolve => setTimeout(resolve, 120000));
  await PushTokenBQ(process.env.REFRESH_TOKEN);
});

//Webhook API Endpoint
app.post('/TA-Intuit',(req,res) => {

 console.log('Received Request:' + JSON.stringify(req.body));
 
 //Some Logic to pull Invoice data or possibly a switch for other types of data
 //GetInvoiceData();
 // Send a response
 res.status(200).send('Webhook received successfully');
   
});

//Endpoint for Reporting Profit and Loss
app.get('/ProfitLoss',async(req, res) => {

  console.log("The Client Secret Is: " + process.env.CLIENT_SECRET);
  console.log("RealmID is: " + process.env.REALM_ID);
  console.log("Client Redirect is: " + process.env.CLIENT_REDIRECT);  
  console.log("The refresh token is " + process.env.REFRESH_TOKEN );

  let Data = await getProfitLossDetailData("Last Fiscal Year","sort_by=Date");

  let Schema = await InferSchemaReport(Data);

  InferData(Data,Schema);

  //console.log(InvoiceSchema);

  //console.log(InferSchema(Data));

  //console.log("\n\nProfit and Loss Data:\n" + PrettyPrint(Data));
  //res.send("\n\nProfit and Loss Data:\n" + PrettyPrint(Data));
  res.send("OK! Check Console");
});

//Refresh Timer used for testing refresh tokens only
app.get('/StartRefreshTimers',(req,res) =>{
  CheckAccessToken();

setInterval(CheckAccessToken, 65 * 60 * 1000);
console.log('Refresh Access Token Timer Started')
});

//Check if Token is still valid, may call Refresh Token if needed
async function CheckAccessToken()
{
   process.env.REFRESH_TOKEN = await GetRefreshTokenBQ();

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

      if(process.env.REFRESH_TOKEN != authResponse.json.refresh_token)
        {
          console.log('\n\n ***Refresh Token has changed*** \n\n');
        }  
        //update Refresh Token Variable
        PushTokenBQ(authResponse.json.refresh_token);

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

    if(process.env.REFRESH_TOKEN != authResponse.json.refresh_token)
    {
      console.log('\n\n ***Refresh Token has changed*** \n\n');
    }
    //update Refresh Token Variable
    PushTokenBQ(authResponse.json.refresh_token);

    //Decrement Option
    FirstRunOption--;

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

async function GetRefreshTokenBQ(TargetTable)
{
  // Define the query to fetch one column and one row
  const query = `
  SELECT Refresh_Token
  FROM \`tableu-442921.${TokenDataSet}.${TokenDataSet}\`
`;

// Define the query options
const options = {
  query: query,
  location: 'US', // Adjust to your dataset's location
};

try {
  // Run the query
  const [rows] = await BQ.query(options);

  // Log the result (it will be an array of rows)
  if (rows.length > 0) {
    console.log('Result:', rows[rows.length-1].Refresh_Token);
    return rows[rows.length-1].Refresh_Token;

  } else {
    console.log('No data found.');
    return null;
  }
} catch (error) {
  console.error('Error Fetching Token:', error);
}
}

async function createTokenTable()
{
  await createTableBQ(TokenDataSet,TokenDataSet,TokenSchema);
}

async function PushTokenBQ(Newtoken)
{
  await PushDataBQ(TokenDataSet,TokenDataSet,[{Refresh_Token: Newtoken}]);
  process.env.REFRESH_TOKEN = Newtoken;
}
  
//Generic API Call Template
async function GetAPICall(baseURL,CompID,query)
{
  console.log(`\nCalling with URL: ${baseURL}v3/company/${CompID}/${query}\n`)

  let ReturnData = null;
  if(await CheckAccessToken())
    {
      await oauthClient
      .makeApiCall({ url: `${baseURL}v3/company/${CompID}/${query}`})
      .then(function (response) {
      //console.log(`\n The response for API call is :${JSON.stringify(response.json)}`);
        ReturnData = response.json;
      })
      .catch(function (e) {
      
      console.error('Request failed with status code:', e.response?.status);
      console.error('Response body:', e.response?.data);
      console.error('Headers:', e.response?.headers);
      console.error('Original error message:', e.message);
      console.error('Intuit Transaction ID:', e.intuit_tid || 'N/A');
      });

      return ReturnData;        
}
else
{
  console.log("Bad Access Token!, unable to access data");
  return null
}


}

//Re-usable functions
function PrettyPrint(Data)
{
  return JSON.stringify(Data, null, 2);
}

//Invoice Object, made these to simplify calls
async function GetInvoiceData()
{
   return await GetAPICall(url,companyID,"query?query=select * from Invoice&minorversion=73");   
}

//This may need to be refactored to inlcude date range
// please do not use this in the meanwhile
async function GetProfitLossData()
{
  return await GetAPICall(url,companyID,"query?query=select * from ProfitLoss&minorversion=73");
}

async function getProfitLossDetailData(date_macro,options)
{
  //Date Macro choices
  /* Today, Yesterday, This Week, Last Week, 
  This Week-to-date, Last Week-to-date, 
  Next Week, Next 4 Weeks, This Month, 
  Last Month, This Month-to-date, Last Month-to-date, 
  Next Month, This Fiscal Quarter, Last Fiscal Quarter, 
  This Fiscal Quarter-to-date, Last Fiscal Quarter-to-date, 
  Next Fiscal Quarter, This Fiscal Year, Last Fiscal Year, 
  This Fiscal Year-to-date, Last Fiscal Year-to-date, Next Fiscal Year */

  //Gets General report info with only date
  if(options != "")
  {
    return await GetAPICall(url,companyID,`reports/ProfitAndLossDetail?date_macro=${date_macro}&minorversion=73`);
  }
  else
  {
    //options are delimited by &
    //return with extra options
    return await GetAPICall(url,companyID,`reports/ProfitAndLossDetail?date_macro=${date_macro}&${options}&minorversion=73`);
  }
  
} 

async function PushInvoiceData(InvoiceData)
{
  if(InvoiceData)
  {
    let TransformedData = TransformJSONInvoice(InvoiceData);
    await createTableBQ("Invoice","Invoice_Detail",InvoiceSchema);
    await PushDataBQ("Invoice","Invoice_Detail",TransformedData);
  }
  else
  {
    console.log("Data not recieved, Data will not be pushed");
  }
  
}

async function ManualTruncate(DataID,TabID)
{
  // Truncate the table
  await BQ.query(`TRUNCATE TABLE \`${DataID}.${TabID}\``);
}

//Transform Data from API call into one that fits Schema(used for streaming data inserts)
async function TransformJSONInvoice(Data)
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
//Push Data onto Google Big Query, below is a streaming insert implementation
async function PushDataBQ(DataID,TabID,RowData)
{
 
  try{
    const options = {
      autodetect: true,
      writeDisposition: 'WRITE_TRUNCATE', 
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



//Push Data, below is a batch loading implementation
async function PushDataBQBatch(DataID,TabID,Data)
{
  
  try {
    const options = {
      sourceFormat: 'NEWLINE_DELIMITED_JSON', // Specify data format
      writeDisposition: 'WRITE_TRUNCATE',    // Overwrite the table
      autodetect: true,                      // Let BigQuery detect the schema
    };

    // Create a Buffer from the newline-delimited JSON string
    const dataString = Data.map(row => JSON.stringify(row)).join('\n');
    const dataBuffer = Buffer.from(dataString, 'utf-8'); // Crucial change
    //const dataBuffer = Readable.from(dataString);
    // Create a readable stream from the string
    const stream = new Readable({
      read() {
        this.push(dataString); 
        this.push(null); // Signal end-of-stream
      }
    });
   

    const [job] = await BQ.dataset(DataID).table(TabID).load(stream, options); // Pass the Buffer


    console.log(`Job ${job.id} completed.`);
    console.log('Batch load successful.');
  } catch (error) {
    console.error('Error loading data into BigQuery:', error);

    // Handle individual row errors, if any
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

    const options = {
      schema: schemaprofile,
      location: 'US',
    };

  
    if (!tableExists) {
      // Create the table if it doesn't exist
      
      const [table] = await dataset.createTable(tableId, options);
      console.log(`Table ${table.id} created.`);
    } else {
 
      console.log(`Table ${tableId} already exists.`);

      /*
      const table = dataset.table(tableId);

      await table.delete({ force: true }); // Use force to delete even if table has data
      console.log(`Table ${datasetId}.${tableId} truncated.`);

      await dataset.createTable(tableId, options);
      console.log(`Table ${table.id} created after truncation `);
      */
      
    }
  } catch (err) {
    console.error('Error creating or checking table:', err);
  }
}

//Create Data Buffer using provided Schema and Data Structure
async function CreateDataBufferString(TransformedData, Schema) {
  let BufferString = '';

  for (let i = 0; i < TransformedData.length; i++) {
    // Open bracket
    BufferString += '{';

    // Loop through each Schema field
    for (let j = 0; j < Schema.length; j++) {
      const fieldName = Schema[j].name;
      const fieldValue = TransformedData[i][fieldName];

      // Add key and value, handling string values
      BufferString += `"${fieldName}":`;
      BufferString += typeof fieldValue === 'string' ? `"${fieldValue}"` : fieldValue;

      // Add comma if not the last field
      if (j !== Schema.length - 1) {
        BufferString += ',';
      }
    }

    // Close bracket and add newline
    BufferString += '}\n';
  }

  return BufferString;
}

let PORT = 3000;
//Listen for request on logs to vs
app.listen(PORT, function () {
  console.log(`Example app listening on port ${PORT}!`);
  console.log(`http://localhost:${PORT}`);
  console.log(`To Start O-Auth Process click on link below`);
  console.log(`http://localhost:${PORT}/Initiate-OAuth`);
  console.log('To get Profit and Loss data without OAuth using refresh tokens click on link below');
  console.log(`http://localhost:${PORT}/ProfitLoss`);
  console.log('To Create Table for token');
  console.log(`http://localhost:${PORT}/CreateTokenStore`);
  console.log('To Upload token stored on env file to BQ');
  console.log(`http://localhost:${PORT}/PushTokenStore`);
  console.log('Combined function from above (a little finicky)');
  console.log(`http://localhost:${PORT}/Store-Keys`);
  console.log('Used to Check Tokens and get New Tokens if needed');
  console.log(`http://localhost:${PORT}/Store-Keys`);

});

/* 
Below 3 functions are for OAuth Purposes for obtaining Tokens, may not be used anymore in favor of refresh tokens
******************************************************************
*/
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
let InvoiceData = await GetInvoiceData();
await PushInvoiceData(InvoiceData);

// Schedule the token refresh to happen every 60 minutes (3600 seconds)
// Old CODE 60 * 60 * 1000
setInterval(RefreshAccessToken, 55 * 60 * 1000);
console.log('Refresh Access Token Timer Started')

//Set Interval for Getting Invoice Data
setInterval(GetInvoiceData,60 * 60 * 1000);


}
/* 
******************************************************************
*/

//General functions for testing GET requests provided by Intuit API
app.get('/Start',async(req, res) => {
  //Check Token and IDs
  console.log("The Client Secret Is: " + process.env.CLIENT_SECRET);
  console.log("RealmID is: " + process.env.REALM_ID);
  console.log("Client Redirect is: " + process.env.CLIENT_REDIRECT);  
  console.log("The refresh token is " + process.env.REFRESH_TOKEN );

})
