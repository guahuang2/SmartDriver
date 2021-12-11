'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]),encoding:'latin1'})

hclient.table('taxi_revenue_byarea').scan(
	{
		filter: {
			type: "PrefixFilter",
			value: "1-1-0-0"
		},
		maxVersions: 1
	},
	function (err, cells) {
		// console.info(cells);
	})

function getAverage(row){
	console.log(row)
	let totalCount=row['total_count']
	for (const [key, value] of Object.entries(row)) {
		if( key!='pickUpArea' &&  key!='total_count'){
			row[key]=(value/totalCount).toFixed(1)
		}
	}
	return row
}

function counterToNumber(c){
	return Number(Buffer.from(c,'latin1').readBigInt64BE());
}

function counterToFloat(c){
	return Number(Buffer.from(c,'latin1').readFloatBE());
}

function getPrefixFromQuery(query){
	return query['month']+'-'+query['day']+'-'+query['hour']+'-'+query['minute']
}

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

// function weather_delay(totals, weather) {
// 	console.info(totals);
// 	let flights = totals[weather + "_flights"];
// 	let delays = totals[weather + "_delays"];
// 	if(flights == 0)
// 		return " - ";
// 	return (delays/flights).toFixed(1); /* One decimal place */
// }

function getPickupArea(input){
	let r = "";
	for (var i = input.length - 1; i >= 0; i--){
		if (input[i]=='-')
			return r;
		else {
			r = r + input[i];
		}
	}
	return r;
}

// function getRow(cell){
// 	let row = {};
// 	row['area']=getPickupArea(cell['pickuparea_date'])
// 	row['total_order_count']=cell['total_order_count'];
// 	row['avg_end_hour']=cell['avg_end_hour'];
// 	row['avg_end_minute']=cell['avg_end_minute'];
// 	row['avg_trip_miles']=cell['avg_trip_miles'];
// 	row['avg_fare']=cell['avg_fare'];
// 	row['avg_tips']=cell['avg_tips'];
// 	row['avg_trip_total']=cell['avg_trip_total'];
// 	return row
// }

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function getFieldName(inputstr){
	// console.log(inputstr);
	let idx=inputstr.indexOf(":")
	return inputstr.substr(idx+1)
}

function groupByArea(cells) {
	// Mustache expects averages, not total flights and delays
	let result = []; // let is a replacement for var that fixes some technical issues
	let rowTotals;
	let lastArea = 0
	cells.forEach(function (cell) {
		// let cellMap=rowToMap(cell);
		let area = Number(getPickupArea(cell['key']));
		if(lastArea !== area) {
			if(rowTotals) {
				result.push(getAverage(rowTotals))
				if (rowTotals['pickUpArea']==1){
					console.log(rowTotals)
				}
			}
			rowTotals = {}
			rowTotals['pickUpArea']=area
		}
		let fieldName;
		fieldName=getFieldName(cell['column'])
		rowTotals[fieldName] = Number(counterToNumber(cell['$']))
		lastArea = area;
	})
	result.push(getAverage(rowTotals))
	return result;
}


app.use(express.static('public'));
app.get('/lookup.html',function (req, res) {
    const prefix=getPrefixFromQuery(req.query);
	// console.log(prefix);
	hclient.table('taxi_revenue_byarea').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: prefix
			},
			maxVersions: 1
		},
		function (err, cells) {
			let template = filesystem.readFileSync("result.mustache").toString();
			let html = mustache.render(template,  { area_avgs: groupByArea(cells)});
		res.send(html);
	})
});

/* Send simulated taxi data to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/submit.html',function (req, res) {
	var area=parseInt(req.query['area']);
	var month = parseInt(req.query['month']);
	var day = parseInt(req.query['day']);
	var hour = parseInt(req.query['hour']);
	var minute = parseInt(req.query['minute']);
	var ehour = parseInt(req.query['ehour']);
	var eminute = parseInt(req.query['eminute']);
	var tripm = parseFloat(req.query['tripm']);
	var fare = parseFloat(req.query['fare']);
	var tips = parseFloat(req.query['tips']);
	var total = parseFloat(req.query['total']);


	var report = {
		area:area,
		month :month,
		day:day,
		hour:hour,
		minute:minute,
		ehour:ehour,
		eminute:eminute,
		tripm:tripm,
		fare:fare,
		tips:tips,
		total:total
	};

	kafkaProducer.send([{ topic: 'guahuang_taxi', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit_taxi.html');
		});
});

app.listen(port);
