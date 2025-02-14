'use strict';
//Requires for using Environment variables
require('dotenv').config();
const { Buffer } = require('buffer');
const { Readable } = require('stream');

//Big query API requires
const {BigQuery, Table} = require('@google-cloud/bigquery');
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
let TokenDataSet = 'IntuitKeys';


//Infer A Schema for a report
async function InferSchemaReport(data)
{
  let InferredSchema = [];
  //Get Column Names and Data Types
  for(let i = 0; i < data.Columns.Column.length; i++)
  {
    InferredSchema.push({ name: removeSpecialCharacters(data.Columns.Column[i].ColTitle), type: ResolveBQType(data.Columns.Column[i].ColType), mode: 'NULLABLE'});
  }
  //Push Extra Columns to Organize data
  InferredSchema.push({ name: 'Classification', type: 'STRING', mode: 'NULLABLE' });
  InferredSchema.push({ name: 'Actual', type: 'FLOAT', mode: 'NULLABLE' });
  InferredSchema.push({ name: 'Projected', type: 'FLOAT', mode: 'NULLABLE' });
  InferredSchema.push({ name: 'Table', type: 'STRING', mode: 'NULLABLE' });
  InferredSchema.push({ name: 'Description', type: 'STRING', mode: 'NULLABLE' });
  InferredSchema.push({name:'Reporting Category', type:'STRING', mode:'NULLABLE'});

  return InferredSchema;
}

//Used for abiding to BQ Naming Standards
function removeSpecialCharacters(input) {
  // Replace all non-alphanumeric characters with a blank space
  if(input !== undefined && input !== null && input != '')
  {
    return input.replace(/[^a-zA-Z0-9 ]/g, '');
  }
  return '';
}

//Used for abiding to BQ Naming Standards
function replaceWhitespaceWithUnderscores(input) {
  // Ensure the input is a valid string
  if (typeof input !== 'string') {
    return '';
  }
  // Replace all whitespace characters with underscores
  return input.replace(/\s+/g, '_');
}

//Resolves Type to their appropriate BQ Type for schema creation
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

//
async function PushData(data,dataschema,DatasetName,Projected)
{
  // Holds Table names
   let InferredTableNames = [];
   //Tree Stack to keep of order
   let TreeStack = [];
   //Second Stack to keep of order
   let MemoryStack = [];


   //Used for Inserting into Total Table, Contains Month/Expense/Revenue
   let Months = Projected.map(item => item.Projected_Month.value);
   let SumActualExpenses = Months.map(item => 0);
   let SumActualRevenue = Months.map(item => 0);

   //Used for Inserting into the Total Table in Big Query
   let TotalTable = [];

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

              //Push Additional Rows for Extra Columns
              TableRow['Classification'] = removeSpecialCharacters(CurrentLevel.Header.ColData[0].value) == 'Sales' ? 'Income' : 'Expense';
              TableRow['Actual'] = TableRow['Amount'];
              TableRow['Projected'] = null;
              TableRow['Table'] = removeSpecialCharacters(CurrentLevel.Header.ColData[0].value);
              TableRow['Description'] = TableRow['MemoDescription'] == null ? TableRow['Transaction Type'] : TableRow['MemoDescription'];
              TableRow['Reporting Category'] = 'Actual';
              //Add Sums
              SumActualExpenses[GetIntegerMonth(TableRow['Date'])-1] += (TableRow['Classification'] == 'Expense' && TableRow['Actual'] != null && TableRow['Amount'] != '' ) ? parseFloat(TableRow['Actual'], 10) : 0;
              SumActualRevenue[GetIntegerMonth(TableRow['Date'])-1] += (TableRow['Classification'] == 'Income' && TableRow['Actual'] != null && TableRow['Amount'] != '') ? parseFloat(TableRow['Actual'], 10) : 0;

              FullTable.push(TableRow);
            }

            //Create Dataset and Table, push contents to BigQuery
            await createTableBQ(DatasetName, removeSpecialCharacters(CurrentLevel.Header.ColData[0].value), dataschema);
            await ManualTruncate(DatasetName, removeSpecialCharacters(CurrentLevel.Header.ColData[0].value));
            await PushDataBQManual(DatasetName, removeSpecialCharacters(CurrentLevel.Header.ColData[0].value), dataschema, FullTable);


            //await PushDataBQ(ProfitLossDataSetName,removeSpecialCharacters(CurrentLevel.Header.ColData[0].value),FullTable);
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

  //Construct Total Table for Inserting into
  for(let i = 0; i < Months.length; i++)
  {
    //Push Actual Expenses
    TotalTable.push({'Date': Months[i], 'Actual': SumActualExpenses[i], 'Projected': null, 'Table': DatasetName + '_Totals','Reporting Category': 'Actual','Classification': 'Total Expense', 'Description': 'Total Expenses'}); 
    TotalTable.push({'Date': Months[i], 'Actual': null, 'Projected': Projected[i].Projected_Expense, 'Table': DatasetName + '_Totals','Reporting Category': 'Projected','Classification':'Total Expense', 'Description': 'Total Expenses'});

    //Push Actual Revenue
    TotalTable.push({'Date': Months[i], 'Actual': SumActualRevenue[i], 'Projected': null, 'Table': DatasetName + '_Totals','Reporting Category': 'Actual','Classification': 'Total Revenue', 'Description': 'Total Revenue'}); 
    TotalTable.push({'Date': Months[i], 'Actual': null, 'Projected': Projected[i].Projected_Revenue, 'Table': DatasetName + '_Totals','Reporting Category': 'Projected','Classification':'Total Revenue', 'Description': 'Total Revenue'});

    //Push Net Income
    TotalTable.push({'Date': Months[i], 'Actual': SumActualRevenue[i] -SumActualExpenses[i], 'Projected': null, 'Table': DatasetName + '_Totals','Reporting Category': 'Actual','Classification': 'Net', 'Description': 'Net'}); 
    TotalTable.push({'Date': Months[i], 'Actual': null, 'Projected': Projected[i].Projected_Revenue - Projected[i].Projected_Expense, 'Table': DatasetName + '_Totals','Reporting Category': 'Projected','Classification':'Net', 'Description': 'Net'});

    //Push Percent
    TotalTable.push({'Date': Months[i], 'Actual': SumActualRevenue[i] == 0 ? 0 : (SumActualRevenue[i] -SumActualExpenses[i]) / SumActualRevenue[i], 'Projected': null, 'Table': DatasetName + '_Totals','Reporting Category': 'Actual','Classification': 'Percent Revenue', 'Description': 'Percent Revenue'}); 
    TotalTable.push({'Date': Months[i], 'Actual': null, 'Projected': (Projected[i].Projected_Revenue - Projected[i].Projected_Expense) / Projected[i].Projected_Revenue, 'Table': DatasetName + '_Totals','Reporting Category': 'Projected','Classification':'Percent Revenue', 'Description': 'Percent Revenue'});
  }

  //Create Tables for Projected and Actual Totals
  await createTableBQ(DatasetName + '_Totals', DatasetName + '_Totals', dataschema);
  await ManualTruncate(DatasetName + '_Totals', DatasetName + '_Totals');
  //Push Total Data onto DataSet
  await PushDataBQManual(DatasetName + '_Totals', DatasetName + '_Totals', dataschema, TotalTable);
  

}

function GetIntegerMonth(date)
{
  // Extract the month
const month = parseInt(date.split("-")[1], 10);
return month;
}

//Parse Customers API for Customer Data
async function GetCustomerData()
{
  return await GetAPICall(url,companyID,"query?query=select * from Customer&minorversion=73");
}

//Used to check and store keys
app.get('/Check-Keys',async (req,res) => {
  CheckAccessToken();

});

app.get('/Store-Keys',async (req,res) => {

  await PushTokenBQ(process.env.REFRESH_TOKEN);
});

//Webhook API Endpoint
app.post('/TA-Intuit',async(req,res) => {
 console.log('Received Request:' + JSON.stringify(req.body));
 await GetProfitLossWrapper(); 
 // Send a response
 res.status(200).send('Webhook received successfully');
});

//Endpoint for Reporting Profit and Loss
app.get('/ProfitLoss',async(req, res) => {

  console.log("The Client Secret Is: " + process.env.CLIENT_SECRET);
  console.log("RealmID is: " + process.env.REALM_ID);
  console.log("Client Redirect is: " + process.env.CLIENT_REDIRECT);  
  console.log("The refresh token is " + process.env.REFRESH_TOKEN );

  await GetProfitLossWrapper();
 
  res.send("OK! Check Console");
});

async function GetProfitLossWrapper()
{
  let Customers = await GetCustomerData();
  let CustomerNames = await GetCustomers(Customers);

  //Get Projected Data
  let ProjectedData = await GetBigQueryData("Bamrec_Projected_2024","Preschool");

  for(let i = 0; i < CustomerNames.length; i++)
  {
    let Data = await getProfitLossDetailData(`Last Fiscal Year&customer=${CustomerNames[i].CustomerID}`,"sort_by=Date");
    //&& Data.Rows.constructor === Object
    if(Object.keys(Data.Rows).length !== 0 )
    {
      let Schema = await InferSchemaReport(Data);
      await PushData(Data,Schema,replaceWhitespaceWithUnderscores(removeSpecialCharacters(CustomerNames[i].CustomerName)) + "_" + CustomerNames[i].CustomerID + "_PNL",ProjectedData);

    }  
  }

  console.log("Get Profit Loss Wrapper Completed");
}

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
    //push new token
    await PushTokenBQ(process.env.REFRESH_TOKEN); 
   }

   

   return oauthClient.isAccessTokenValid();

}

//Gets Customers that are not projects, should return an array of objects
async function GetCustomers(Data)
{
  let Customer = [];
  if(Data.QueryResponse != undefined)
  {
    if(Data.QueryResponse.Customer.length > 0)
      {
        for(let i = 0; i < Data.QueryResponse.Customer.length; i++)
        {
          Customer.push({CustomerID: Data.QueryResponse.Customer[i].Id, CustomerName: Data.QueryResponse.Customer[i].FullyQualifiedName.replace(/:/g, " ")});
        }
      }
  }
   return Customer
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
        process.env.REFRESH_TOKEN = authResponse.json.refresh_token;

        //push new token
        //PushTokenBQ(process.env.REFRESH_TOKEN);

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
    process.env.REFRESH_TOKEN = authResponse.json.refresh_token;

    //push new token
    //PushTokenBQ(process.env.REFRESH_TOKEN); 
    

    

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
  await createTableBQ(TokenDataSet,TokenDataSet,TokenSchema);
  await ManualTruncate(TokenDataSet,TokenDataSet);
  await PushDataBQManualSingle(TokenDataSet,TokenDataSet,TokenSchema,{Refresh_Token: Newtoken});
  process.env.REFRESH_TOKEN = Newtoken;
}

//Gets Projected Data to create new Tables
async function GetBigQueryData(DatasetID, TableID)
{
    // Construct the query
  const query = `
  SELECT *
  FROM \`${DatasetID}.${TableID}\`
`;

try {
  // Execute the query
  const [rows] = await BQ.query({ query });
  return rows;
} catch (err) {
  console.error('ERROR:', err);
}


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

//Customer Object call
async function GetCustomerData()
{
  return await GetAPICall(url,companyID,"query?query=select * from Customer&minorversion=73");
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

//Code to manually truncate table
//DO NOT USE WITH STREAMING INSERTS
async function ManualTruncate(DataID,TabID)
{
  // Truncate the table
  await BQ.query(`TRUNCATE TABLE \`${DataID}.${TabID}\``);
  console.log("Table Truncated");
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

//Alternative push data to BQ, creates a manual query string
async function PushDataBQManual(datasetId, tableId, schema, rows) {
  try {
    // Step 1: Construct the INSERT query for multiple rows
    //const columns = schema.map(field => field.name).join(', ');
    const columns = schema.map(field => `\`${field.name}\``).join(', '); // Wrap column names in backticks

    // Generate values for each row
    const valuesArray = rows.map(row => {
      const values = schema
        .map(field => {
          const value = row[field.name];
          // Dynamically format values based on their type
        if (field.type === 'STRING') {
          //return `'${value.replace(/'/g, "\\'")}'`; // Escape single quotes for SQL
          return `'${removeSpecialCharacters(value)}'`;
        }  else if (field.type === 'DATE') {
          return `'${value}'`; // Dates should be passed as strings
        } else if ((field.type === 'NUMERIC' || field.type === 'FLOAT' || field.type === 'INTEGER') && value != null) {
          return value; // Pass numeric values as-is
        } else if (value === null || value === undefined) {
          return 'NULL';
        } else {
          return value;
        }


        })
        .join(', ');
      return `(${values})`;
    });

    const valuesString = valuesArray.join(',\n'); // Combine rows into one query
    const insertQuery = `INSERT INTO \`${datasetId}.${tableId}\` (${columns}) VALUES ${valuesString}`;

    // Step 2: Execute the query
    //console.log(`Executing query: ${insertQuery}`);
    await BQ.query(insertQuery);
    //console.log('Rows inserted successfully.');
  } catch (error) {
    console.error('Error during insertion:', error);
  }
}

//Other Alternative to push, pushes a single row to manual query string
async function PushDataBQManualSingle(datasetId, tableId, schema, row) {
  try {
    // Step 1: Construct the INSERT query
    const columns = schema.map(field => `\`${field.name}\``).join(', '); // Wrap column names in backticks
    //const columns = schema.map(field => field.name).join(', ');
    const values = schema
      .map(field => {
        const value = row[field.name];
        // Dynamically format values based on their type
        if (field.type === 'STRING') {
          //return `'${value.replace(/'/g, "\\'")}'`; // Escape single quotes for SQL
          return `'${removeSpecialCharacters(value)}'`;
        }  else if (field.type === 'DATE') {
          return `'${value}'`; // Dates should be passed as strings
        } else if (field.type === 'NUMERIC' || field.type === 'FLOAT' || field.type === 'INTEGER') {
          return value; // Pass numeric values as-is
        } else if (value === null || value === undefined) {
          return 'NULL';
        } else {
          return value;
        }
      })
      .join(', ');

    const insertQuery = `INSERT INTO \`${datasetId}.${tableId}\` (${columns}) VALUES (${values})`;

    // Step 2: Execute the query
    console.log(`Executing query: ${insertQuery}`);
    await BQ.query(insertQuery);
    console.log('Row inserted successfully.');
  } catch (error) {
    console.error('Error during insertion:', error);
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
  console.log('To get Profit and Loss data without OAuth using refresh tokens click on link below');
  console.log(`http://localhost:${PORT}/ProfitLoss`);
  console.log('To Push Tokens from NodeJS Application to BigQuery');
  console.log(`http://localhost:${PORT}/Store-Keys`);
  console.log('Check Keys, Push New Key to BigQuery');
  console.log(`http://localhost:${PORT}/Check-Keys`);
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

res.send("Keys Updated through OAUTH")
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
  console.log("The refresh token in .env file is " + process.env.REFRESH_TOKEN );

})
