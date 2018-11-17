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
from cognite.config import configure_session
from cognite.v05 import timeseries
from cognite.v05 import assets
from cognite import _utils

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

def create_assets_and_timeseries(data, root):
	for cryptocurrency in data['data']:
		asset = assets.Asset(name=cryptocurrency['symbol'],
			parent_id=root,
			description=cryptocurrency['name'])
		assetList = [asset]
		response = assets.post_assets(assetList)
		tsPrice = timeseries.TimeSeries(name=cryptocurrency['symbol']+'/price/USD', unit='USD', asset_id=response.to_json()[0]['id'],description=cryptocurrency['name']+' price')
		tsVol = timeseries.TimeSeries(name=cryptocurrency['symbol']+'/vol/24h/USD', unit='USD', asset_id=response.to_json()[0]['id'],description=cryptocurrency['name']+' volume last 24h')
		tsCap = timeseries.TimeSeries(name=cryptocurrency['symbol']+'/cap/USD', unit='USD', asset_id=response.to_json()[0]['id'],description=cryptocurrency['name']+' market cap')
		tsList = [tsPrice, tsVol, tsCap]
		res2 = timeseries.post_time_series(tsList)

def update_datapoints(data):
	tsPointList = []
	for cryptocurrency in data['data']:
		print(cryptocurrency['quote']['USD']['last_updated'])
		lastUpdated = int(numpy.datetime64(cryptocurrency['quote']['USD']['last_updated']).view('<i8'))
		print(lastUpdated)
		price = cryptocurrency['quote']['USD']['price']
		vol = cryptocurrency['quote']['USD']['volume_24h']
		cap = cryptocurrency['quote']['USD']['market_cap']
		tsPointList.append(timeseries.TimeseriesWithDatapoints(name=cryptocurrency['symbol']+'/price/USD',datapoints=[timeseries.Datapoint(timestamp=lastUpdated, value=price)]))
		tsPointList.append(timeseries.TimeseriesWithDatapoints(name=cryptocurrency['symbol']+'/vol/24h/USD',datapoints=[timeseries.Datapoint(timestamp=lastUpdated, value=vol)]))
		tsPointList.append(timeseries.TimeseriesWithDatapoints(name=cryptocurrency['symbol']+'/cap/USD',datapoints=[timeseries.Datapoint(timestamp=lastUpdated, value=cap)]))
		if len(tsPointList) > 500:
			try:
				timeseries.post_multi_tag_datapoints(tsPointList)
			except _utils.APIError as err:
				logging.info(err)
			tsPointList = []
	if len(tsPointList) > 0:
		try:
			timeseries.post_multi_tag_datapoints(tsPointList)
		except _utils.APIError as err:
			logging.info(err)

if __name__ == "__main__":
    logging.basicConfig(filename='coinmarketcap.log',level=logging.INFO)
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '-k', '--key', type=str, required=True, help='Cognite API key. Required.')
    parser.add_argument(
        '-b', '--bitcoin_key', type=str, required=True, help='Bitcoin API key. Required.')
    parser.add_argument(
        '-p', '--project', type=str, required=True, help='Project name/id. Required.')
    parser.add_argument(
        '-f', '--file', type=str, help='File data from last load.')
    parser.add_argument(
    	'-a', '--asset', type=str, required=True, help='Add assets under this root asset.')
    parser.add_argument(
    	'-c', '--create', type=bool, help='Create assets and time series. First run only.')
    
    args = parser.parse_args()
    configure_session(args.key, args.project)

    data = []
    if args.file:
    	data = get_cmc_from_file(args.file)
    else:
    	data = download_cmc(args.bitcoin_key)
    	save_cmc_to_file(data, 'coinmarketcap.last.json')

    if args.create:
    	create_assets_and_timeseries(data, args.asset)

    update_datapoints(data)