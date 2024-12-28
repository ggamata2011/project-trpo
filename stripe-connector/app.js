// Requiring environment variables
require('dotenv').config();

// Imports the Google Cloud client library
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize BigQuery client
const bigquery = new BigQuery({
    //keyFilename: 'tableu-442921-2589eb103d9b.json', // Path to your service account key file
    //keyFilename: 'stripe-bamrec-767863fb1b1a.json'
    //keyFilename: 'tableu-442921-272d860b3fc9.json',
    keyFilename: 'stripe-bamrec-a1e0f5c2bec0'
});

// Initialize the app with Express
const express = require('express');
const app = express();

// Body-parser for parsing and organization
const bodyParser = require('body-parser');
app.use(bodyParser.json());

// Connecting to the Stripe API using the secret key
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
//console.log(process.env.STRIPE_SECRET_KEY);

if (!process.env.STRIPE_SECRET_KEY) {
    throw new error('STRIPE_SECRET_KEY is not defined');
}

// Function to ensure dataset and table exist in BigQuery
async function ensureDatasetAndTable(datasetId, tableId) {
    try {
        // Check or create the dataset
        const datasets = await bigquery.getDatasets();
        const datasetExists = datasets[0].some(ds => ds.id === datasetId);
        if (!datasetExists) {
            await bigquery.createDataset(datasetId);
            console.log(`Dataset "${datasetId}" created.`);
        } else {
            console.log(`Dataset "${datasetId}" already exists.`);
        }

        // Check or create the table
        const dataset = bigquery.dataset(datasetId);
        const [tables] = await dataset.getTables();
        const tableExists = tables.some(table => table.id === tableId);
        if (!tableExists) {
            const schema = [
                { name: 'id', type: 'STRING' },
                { name: 'object', type: 'STRING' },
                { name: 'amount', type: 'FLOAT' },
                { name: 'currency', type: 'STRING' },
                { name: 'status', type: 'STRING' },
                { name: 'created', type: 'TIMESTAMP' },
                { name: 'description', type: 'STRING' },
                //{ name: 'customer', type: 'STRING' }
            ];
            await dataset.createTable(tableId, { schema });
            console.log(`Table "${tableId}" created.`);
        } else {
            console.log(`Table "${tableId}" already exists.`);
        }
    } catch (err) {
        console.error('Error ensuring dataset and table:', err);
    }
}

// Function to insert rows into BigQuery
async function insertIntoBigQuery(datasetId, tableId, rows) {
    try {
        await bigquery.dataset(datasetId).table(tableId).insert(rows);
        console.log(`Inserted ${rows.length} rows into BigQuery table: ${tableId}`);
    } catch (err) {
        console.error('Error inserting rows:', err);
    }
}



// Endpoint to fetch and insert Stripe transactions into BigQuery
app.get('/transactions', async (req, res) => {
    WrapperFunction(res);
});
/*
//list out transactions on console
(async () => {
    try {
        const charges = await stripe.charges.list({limit:3})
        console.log('Here are the recent BamRec Transactions!');
        console.log(charges);
    } catch (error) {
        console.error("Error fetching charges: ", error);
    }
})(); 
*/

async function WrapperFunction(res)
{
    try {
        const charges = await stripe.charges.list({ limit: 10 });

        // Format data for BigQuery
        const rows = charges.data.map(charge => ({
            id: charge.id,
            object: charge.object,
            amount: charge.amount / 100, // Convert amount to dollars if needed
            currency: charge.currency,
            status: charge.status,
            created: new Date(charge.created * 1000).toISOString(), // Convert UNIX timestamp to ISO format
            description: charge.description || null,
            //customer: charge.customer || 'unknown'
        }));

        // Define dataset and table names
        const datasetId = 'Stripe';
        const tableId = 'Transactions';

        // Ensure dataset and table exist
        await ensureDatasetAndTable(datasetId, tableId);

        // Insert data into BigQuery
        await insertIntoBigQuery(datasetId, tableId, rows);
        res.send('Transaction data loaded into BigQuery!');
    } catch (error) {
        console.error('Error loading transactions:', error);
        res.status(500).send('Failed to load transaction data.');
    }

}

//Webhook API endpoint
app.post('/stripe-wh', (req,res) => {
    console.log('Received Request: ' + req.body);
    console.log(`Data Received ${JSON.stringify(res.json)}`);
    
    WrapperFunction(res);

    console.log("Table Connected");

    res.status(200).send("Webhook Connected");

});


//asynchronous with await
async function endpointfunction(endres)
{


  await ensureDatasetAndTable();
  
  await insertIntoBigQuery();
  
  res.status(200).send("Webhook Connected");

}

// Starting server
app.listen(3001, function () {
    console.log('Server running on port 3001');
    console.log('Visit: http://localhost:3001/transactions to fetch and load Stripe transactions.');
});
