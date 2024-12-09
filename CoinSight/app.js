'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]);
const hbase = require('hbase');
require('dotenv').config()
const port = Number(process.argv[2]); 
const WebSocket = require('ws');
app.use(express.static('public'));

/* Send market data from coinbase feed to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[4]});
var kafkaProducer = new Producer(kafkaClient);

// Set HBase connection parameters
var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: 8090,
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

// Create web socket server and listen on 8890 for client (front end to connect)
// This is used to send coinbase data feed to front end
const server = http.createServer(app);
const WebSocketServer = require('ws').Server;
const wss = new WebSocketServer({ server });
const clientSocketPort = 8890;

server.listen(clientSocketPort, () => {
	console.log(`Server is listening on port ${clientSocketPort}`);
});

let clients = [];

wss.on('connection', function connection(ws) {
	console.log('New WebSocket connection');
	clients.push(ws);

	ws.on('close', function close() {
		console.log('WebSocket connection closed');
		clients = clients.filter(client => client !== ws);
	});
});

// Connect to Coinbase websocket to get market data feed
const URI = 'wss://ws-feed.exchange.coinbase.com';
const channel = 'ticker';
const product_ids = ['BTC-USD', 'ETH-USD'];
const subscribeMessage = JSON.stringify({
	type: 'subscribe',
	channels: [{ name: channel, product_ids: product_ids }],
});
const wsCoinbase = new WebSocket(URI);

wsCoinbase.on('open', function open() {
	console.log('Connected to Coinbase WebSocket');
	wsCoinbase.send(subscribeMessage);
});

wsCoinbase.on('message', function incoming(data) {
	const jsonResponse = JSON.parse(data);

	if (jsonResponse.type === 'ticker') {
		console.log('Received data from Coinbase:', jsonResponse);
		const backEndData = {
			pair: jsonResponse.product_id.replace("-", "").toLowerCase(),
			price_date: new Date(jsonResponse.time).toISOString().split('T')[0],
			close: jsonResponse.price,
			volume: jsonResponse.volume_24h,
		};
		console.log('Backend data: ', backEndData);

		const frontEndData = {
			pair: jsonResponse.product_id,
			price_date: new Date(jsonResponse.time).toLocaleString('en-US', { 
                year: 'numeric', 
                month: 'long', 
                day: 'numeric', 
                hour: '2-digit', 
                minute: '2-digit', 
                second: '2-digit' 
            }),
			close: jsonResponse.price,
			volume: jsonResponse.last_size,
		};

		// Send Coinbase data to Kafka topic
		kafkaProducer.send([{ topic: 'brianchan_crypto_topic', messages: JSON.stringify(backEndData)}],
		function (err, data) {
			//console.log(err);
			//console.log(data);
		});

		// Send Coinbase data to front end
		clients.forEach(client => {
			client.send(JSON.stringify(frontEndData));
		});
	}
});

wsCoinbase.on('close', function close() {
	console.log('Coinbase WebSocket connection closed, retrying..');
	setTimeout(() => {
		wsCoinbase = new WebSocket(URI);
	}, 1000);
});

wsCoinbase.on('error', function error(err) {
	console.error('Coinbase WebSocket error:', err);
});

// Endpoint to get serving layer / batch view market data and render to front end
app.get('/prices.html',function (req, res) {
	const pair = req.query['pair'];
	const startDate = req.query['start_date'];
	const endDate = req.query['end_date'];

	if (!pair || !startDate || !endDate) {
		return res.status(400).send('Missing required parameters');
	}

	const startRow = `${pair}_${startDate}`;
	// Add \x00 to include the end date in results
	const stopRow = `${pair}_${endDate}\x00`;

	console.log(startRow);
	console.log(stopRow);

	// HBase scan to get the relevant pair's data based on start date and end date
	hclient.table('brianchan_crypto_prices_minimal_hbase')
		.scan({
			startRow: startRow,
			endRow: stopRow,
			maxVersions: 1
		}, function(err, rows) {
			if (err) {
				console.error('HBase scan error:', err);
				return res.status(500).send('Error retrieving price data');
			}
			console.log(rows);

			const restructureData = (rawData) => {
				// Group by date using reduce
				const grouped = rawData.reduce((acc, item) => {
					const pair_date = item.key;

					if (!acc[pair_date]) {
						acc[pair_date] = {};
					}

					// Extract column name by removing 'cf:' prefix
					const columnName = item.column.replace('cf:', '');
					acc[pair_date][columnName] = item.$;

					return acc;
				}, {});

				// Convert to array of objects
				return Object.values(grouped);
			};

			const priceData = restructureData(rows);

			console.log('Found prices:', priceData);

			const template = filesystem.readFileSync("result.mustache").toString();
			const html = mustache.render(template, {
				prices: priceData,
				pair: pair,
				startDate: startDate,
				endDate: endDate
			});
			res.send(html);
		});
});

app.listen(port);