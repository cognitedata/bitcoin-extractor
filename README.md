# bitcoin-extractor
Example of how to use the Coinmarketcap API and the Cognite SDK and APIs to store market data about cryptocurrencies

## Dependencies

Install the Cognite Python SDK first
`pip install cognite-sdk`

## Usage

First, create a root asset for Cryptocurrencies. In Postman or a similar tool, POST to create an asset using [this API call](https://doc.cognitedata.com/api/0.5/#operation/postAssets). Store the ID of the asset created by this call.

First run, create an asset hierarchy and time series metadata:

`python3 coinmarketcap.py -k <Cognite API key> -p <Cognite project> -a <Asset ID from call above> -c True -b <Coinmarketcap API key>`

Then, periodically run:

`python3 coinmarketcap.py -k <Cognite API key> -p <Cognite project> -a <Asset ID from call abovce> -b <Coinmarketcap API key>`
