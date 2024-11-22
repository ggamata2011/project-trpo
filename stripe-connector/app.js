//Requiring environment variables
require('dotenv').config({ path: '../.env'});

//Initiate with App with Express
var express = require('express');
var app = express();


//Body-parser for parsing and organization
const bodyParse = require('body-parser');


//Connecting to secret key
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

//List out customers on the console
/*(async () => {
    const customers = await stripe.customers.list({limit: 3});
    console.log('Hello, Authentication');
    console.log(customers); //Print out customers
})()
*/

//list out last 10 transactions on the console
 (async () => {
    try {
        const charges = await stripe.charges.list({limit:3})
        console.log('Here are the recent BamRec Transactions!');
        console.log(charges);
    } catch (error) {
        console.error("Error fetching charges: ", error);
    }
})(); 

// Starting server
app.listen(3001, function () {
    console.log('Server running on port 3001');
    console.log('https://localhost:3001');
});