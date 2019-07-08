#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deletes a given list of time series
"""

import requests
import urllib
import json
import argparse
import logging
import numpy
import sys
import os
import time
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes import TimeSeriesUpdate
from cognite.client.data_classes import Asset

def save_cmc_to_file(data, fn):
    with open(fn, 'w') as outfile:
        json.dump({'items': data}, outfile)

def get_cmc_from_file(fn):
    fileObj = open(fn)
    data = json.load(fileObj)
    return(data['items'])

def download_cmc(apiKey):
	url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?start=1&limit=100&convert=USD"
	try:
		r = requests.get(url = url, headers = {'X-CMC_PRO_API_KEY': apiKey })
		data = r.json()
		logging.info(data)
	except:
		logging.error('Failed to get from coinmarketcap')
	return data

def create_asset_and_timeseries(ext_id, name, symbol, asset_ext_id, root, client):
	res = []
	try:
		res = client.assets.retrieve(external_id=asset_ext_id)
	except CogniteAPIError as e:
		if e.code == 400:
			asset = Asset(external_id=asset_ext_id, name=symbol, parent_id=root, description=name)
			res = client.assets.create(asset)
	print(res)
	ts = client.time_series.create(TimeSeries(external_id=ext_id, name=name, unit='USD', asset_id=res.id, legacy_name=ext_id))
	return ts

def update_or_create_ts(old_name, ext_id, name, symbol, asset_ext_id, root, client):
	try:
		update = TimeSeriesUpdate(external_id=old_name).external_id.set(ext_id).name.set(name)
		ts = client.time_series.update(update)
	except CogniteAPIError as e:
		if e.code == 400:
			return create_asset_and_timeseries(ext_id, name, symbol, asset_ext_id, root, client)

def get_update_or_create_ts(old_name, ext_id, name, symbol, asset_ext_id, root, client):
	try:
		print(ext_id)
		ts = client.time_series.retrieve(external_id=ext_id)
		return ts
	except CogniteAPIError as e:
		if e.code == 400:
			return update_or_create_ts(old_name, ext_id, name, symbol, asset_ext_id, root, client)
		logging.error('Unknown error while retrieving ts')

def update_datapoints(data, root, client):
	tsPointList = []
	num = 0
	for cryptocurrency in data['data']:
		num = num + 1
		print(cryptocurrency['quote']['USD']['last_updated'])
		lastUpdated = int(numpy.datetime64(cryptocurrency['quote']['USD']['last_updated']).view('<i8'))
		print(lastUpdated)
		price = cryptocurrency['quote']['USD']['price']
		vol = cryptocurrency['quote']['USD']['volume_24h']
		cap = cryptocurrency['quote']['USD']['market_cap']
		ts = get_update_or_create_ts(cryptocurrency['symbol']+'/price/USD', 'coinmarketcapid:'+str(cryptocurrency['id'])+'/price/USD', cryptocurrency['name']+' price', cryptocurrency['symbol'], 'coinmarketcapid:'+str(cryptocurrency['id']), root, client)
		ts = get_update_or_create_ts(cryptocurrency['symbol']+'/vol/24h/USD', 'coinmarketcapid:'+str(cryptocurrency['id'])+'/vol/24h/USD', cryptocurrency['name']+' volume', cryptocurrency['symbol'], 'coinmarketcapid:'+str(cryptocurrency['id']), root, client)
		ts = get_update_or_create_ts(cryptocurrency['symbol']+'/cap/USD', 'coinmarketcapid:'+str(cryptocurrency['id'])+'/cap/USD', cryptocurrency['name']+' market cap', cryptocurrency['symbol'], 'coinmarketcapid:'+str(cryptocurrency['id']), root, client)

		tsPointList.append({"externalId": 'coinmarketcapid:'+str(cryptocurrency['id'])+'/price/USD', "datapoints": [(lastUpdated, price)]})
		tsPointList.append({"externalId": 'coinmarketcapid:'+str(cryptocurrency['id'])+'/vol/24h/USD', "datapoints": [(lastUpdated, vol)]})
		tsPointList.append({"externalId": 'coinmarketcapid:'+str(cryptocurrency['id'])+'/cap/USD', "datapoints": [(lastUpdated, cap)]})
	if len(tsPointList) > 0:
		client.datapoints.insert_multiple(tsPointList)

def do_cmc_backfill(apiKey, ids, start, end, client):
	idList = ids.split(',')
	tsPointList = []
	for id in idList:
		curr_time = start
		data_points = []
		while curr_time < end:
			url = "https://pro-api.coinmarketcap.com/v1/tools/price-conversion?id=" + id + "&amount=1&convert=USD&time=" + str(curr_time)
			try:
				r = requests.get(url = url, headers = {'X-CMC_PRO_API_KEY': apiKey })
				data = r.json()
				logging.info(data)
				cryptocurrency = data['data']
				lastUpdated = int(numpy.datetime64(cryptocurrency['quote']['USD']['last_updated']).view('<i8'))
				price = cryptocurrency['quote']['USD']['price']
				data_points.append((lastUpdated, price))
				curr_time += 300
			except:
				logging.error('Failed to get from coinmarketcap, time ', curr_time, ' id ', id)
			time.sleep(2)
		tsPointList.append({"externalId": 'coinmarketcapid:'+id+'/price/USD', "datapoints": data_points})
	client.datapoints.insert_multiple(tsPointList)

if __name__ == "__main__":
    logging.basicConfig(filename='coinmarketcap.log',level=logging.INFO)
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '-k', '--key', type=str, required=True, help='Cognite API key. Required.')
    parser.add_argument(
		'-p', '--project', type=str, required=True, help='CDF project to authenticate towards')
    parser.add_argument(
		'-b', '--bitcoin_key', type=str, required=True, help='Bitcoin API key. Required.')
    parser.add_argument(
		'-f', '--file', type=str, help='File data from last load.')
    parser.add_argument(
		'-a', '--asset', type=str, required=True, help='Add assets under this root asset.')
    parser.add_argument(
    	'-i', '--ids', type=str, help='If set, historical backfill of this comma-separated list of cryptocurrency IDs.')
    parser.add_argument(
    	'-s', '--start', type=str, help='Start timestamp of historical backfill.')
    parser.add_argument(
    	'-e', '--end', type=str, help='End timestamp of historical backfill.')
    
    args = parser.parse_args()
    client = CogniteClient(api_key=args.key, project=args.project, client_name="geir-bitcoin-extractor")

    if args.ids:
    	do_cmc_backfill(args.bitcoin_key, args.ids, int(args.start), int(args.end), client)
    else:
	    data = []
	    if args.file:
	    	data = get_cmc_from_file(args.file)
	    else:
	    	data = download_cmc(args.bitcoin_key)
	    	save_cmc_to_file(data, 'coinmarketcap.last.json')

	    update_datapoints(data, int(args.asset), client)
