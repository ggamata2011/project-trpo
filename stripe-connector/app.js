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
(async () => {
    const customers = await stripe.customers.list();
    console.log('Hello, Authentication');
    console.log(customers); //Print out customers
})()

// Starting server
app.listen(3002, function () {
    console.log('Server running on port 3001');
    console.log('https://localhost:3001');
});