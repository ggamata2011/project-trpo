'use strict';
//Requires for using Environment variables
require('dotenv').config();

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
  environment: 'sandbox',
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

   let ResData = "";

   let Data = await GetInvoiceData();
  
   console.log("\n\nInvoice Data:\n" + PrettyPrint(Data));
   ResData += "\n\nInvoice Data:\n" + PrettyPrint(Data);

   Data = await GetAccountData();
   console.log("\n\nAccount Data:\n" + PrettyPrint(Data));
   ResData += "\n\nAccount Data:\n" + PrettyPrint(Data);

   Data = await GetBillData();
   console.log("\n\nBill Data:\n" + PrettyPrint(Data));
   ResData += "\n\nBill Data:\n" + PrettyPrint(Data);

   Data = await GetCompanyData();
   console.log("\n\nCompany Data:\n" + PrettyPrint(Data));
   ResData += "\n\nCompany Data:\n" + PrettyPrint(Data);

   Data = await GetCustomerData();
   console.log("\n\nCustomer Data:\n" + PrettyPrint(Data));
   ResData += "\n\nCustomer Data:\n" + PrettyPrint(Data);

   Data = await GetEmployeeData();
   console.log("\n\nEmployee Data:\n" + PrettyPrint(Data));
   ResData += "\n\nEmployee Data:\n" + PrettyPrint(Data);

   Data = await GetEstimateData();
   console.log("\n\nEstimate Data:\n" + PrettyPrint(Data));
   ResData += "\n\nEstimate Data:\n" + PrettyPrint(Data);
   
   Data = await GetItemData();
   console.log("\n\nItem Data:\n" + PrettyPrint(Data)); 
   ResData += "\n\nItem Data:\n" + PrettyPrint(Data);

   Data = await GetPaymentData();
   console.log("\n\nPayment Data:\n" + PrettyPrint(Data));
   ResData += "\n\nPayment Data:\n" + PrettyPrint(Data);

   Data = await getTaxAgencyData();
   console.log("\n\nTax Agency Data:\n" + PrettyPrint(Data));
   ResData += "\n\nTax Agency Data:\n" + PrettyPrint(Data);

   Data = await getVendorData();
   console.log("\n\nVendor Data:\n" + PrettyPrint(Data));
   ResData += "\n\nVendor Data:\n" + PrettyPrint(Data);

   Data = await GetAccountListData();
   console.log("\n\nAccount List Data:\n" + PrettyPrint(Data));
   ResData += "\n\nAccount List Data:\n" + PrettyPrint(Data);

   //Unfunctionized code, will be used in future versions
   
  //NOTE: All Reports have customizable queries that we will need to discuss
  // ************************************************************************** 
  //Query AccountListDetail Report
  ResData += ("\n\nAccountListDetail:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/AccountList?columns=account_name,account_type&account_type=Income&minorversion=73")));

  //Query APAgingDetail Report
   ResData += ("\n\nAPAgingDetail:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/AgedPayableDetail?report_date=2015-06-30&start_duedate=2015-01-01&end_duedate=2015-06-30&columns=due_date,vend_name&minorversion=73")));

  //Query APAgingSummary Report
   ResData += ("\n\nAPAgingSummary:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/AgedPayables?date_macro=Today&minorversion=73")));

  //Query ARAgingDetail Report
   ResData += ("\n\nARAgingDetail:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/AgedReceivableDetail?report_date=2015-06-30&start_duedate=2015-01-01&end_duedate=2015-06-30&columns=due_date,cust_name&minorversion=73")));

   //Query ARAgingSummary Report
   ResData += ("\n\nARAgingSummary:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/AgedReceivables?customer=4&date_macro=Last Fiscal Year&minorversion=73")));

   //Query Attachable Object
   ResData += ("\n\nAttachable:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=select * from attachable&minorversion=73")));

   //Query BalanceSheet Report
   ResData += ("\n\nBalanceSheet:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/BalanceSheet?date_macro=Last Fiscal Year-to-date&minorversion=73")));

   //Query Bill Object
   ResData += ("\n\nBill:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=select * from bill&minorversion=73")));

   //Query BillPayment Object
   ResData += ("\n\nBillPayment:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=select * from billpayment&minorversion=73")));

   //Query budget Object
   ResData += ("\n\nbudget:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=Select * from Budget&minorversion=73")));

   //Query CashFlow Object
   ResData += ("\n\nCashFlow:\n" + PrettyPrint(await GetAPICall(url,companyID,"reports/CashFlow?minorversion=73")));

   //Query ChangedDataCapture Report
   //Queries for changed data from a certain date
   //will need to be customized
   ResData += ("\n\nChangedDataCapture:\n" + PrettyPrint(await GetAPICall(url,companyID,"cdc?entities=Customer,Estimate&changedSince=2015-11-28&minorversion=73")));

   //Queries Class Object
   ResData += ("\n\nClass:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=select  * from Class&minorversion=73")));

   //Query CompanyCurrency Object
   ResData += ("\n\nCompanyCurrency:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=select * from companycurrency&minorversion=73")));

   //Query Credit Memo Object
   ResData += ("\n\nCreditMemo:\n" + PrettyPrint(await GetAPICall(url,companyID,"query?query=Select * from CreditMemo&minorversion=73")));

   /*
   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));

   ResData += ("\n\nPLaceholder:\n" + PrettyPrint(await GetAPICall(url,companyID,"")));
   */


   res.send("Test Page Send for Refresh Token Accessed, displaying data in console log, and sending response\n\n" + ResData);

   //push Data to big query
   //await PushInvoiceData(Data);
});
   
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

//Account Object
async function GetAccountData()
{
  return  await GetAPICall(url,companyID,"query?query=select * from Account&minorversion=73");
}

//Get Account List Detail
async function GetAccountListData()
{
  return  await GetAPICall(url,companyID,"reports/AccountList?columns=account_name,account_type&account_type=Income&minorversion=73");
}

//Bill Object
async function GetBillData()
{
  return await GetAPICall(url,companyID,"query?query=select * from bill&minorversion=73");
}

//Get CompanyInfo Object
async function GetCompanyData()
{
  return await GetAPICall(url,companyID,"query?query=select * from CompanyInfo&minorversion=73");
}

//Get Customer Object
async function GetCustomerData()
{
  return await GetAPICall(url,companyID,"query?query=select * from Customer&minorversion=73");
}

//Get Employee Object
async function GetEmployeeData()
{
  return  await GetAPICall(url,companyID,"query?query=select * from Employee&minorversion=73");
}

//Get Estimate Object
async function GetEstimateData()
{
  return await GetAPICall(url,companyID,"query?query=select * from estimate&minorversion=73");
}

//get Item Object
async function GetItemData()
{
  return await GetAPICall(url,companyID,"query?query=select * from Item&minorversion=73");
}

//Get Payment Object
async function GetPaymentData()
{
  return await GetAPICall(url,companyID,"query?query=select * from Payment&minorversion=73");
}


//This may need to be refactored to inlcude date range
// please do not use this in the meanwhile
async function GetProfitLossData()
{
  return await GetAPICall(url,companyID,"query?query=select * from ProfitLoss&minorversion=73");
}

async function getTaxAgencyData()
{
  return await GetAPICall(url,companyID,"query?query=select * from TaxAgency&minorversion=73");
}

async function getVendorData()
{
  return await GetAPICall(url,companyID,"query?query=select * from vendor&minorversion=73");
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





