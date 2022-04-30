import streamlit as st
import pandas as pd
import numpy as np
from os import environ
from coinmetrics.api_client import CoinMetricsClient
import matplotlib.pyplot as plt
import ssl

pd.options.display.float_format = '{:,}'.format

ssl._create_default_https_context = ssl._create_unverified_context

st.set_page_config(layout="wide")
st.title('Ethereum Historical Metrics')

CONTRACTS_ERC20_BY_MONTH_URL = 'https://storage.googleapis.com/bda_eth/outputs/contracts/contracts_erc20_by_month.csv/part-00000-cded6257-eda9-4201-beb3-362221cf848d-c000.csv'
CONTRACTS_ERC721_BY_MONTH_URL = 'https://storage.googleapis.com/bda_eth/outputs/contracts/contracts_erc721_by_month.csv/part-00000-15ca50ec-f016-4766-a038-03624815d269-c000.csv'
TXNS_BY_MONTH_AND_TYPE_URL= 'https://storage.googleapis.com/bda_eth/outputs/transactions/transactions_by_month_and_type/part-00000-a1e38436-8ccc-4b89-ae82-8614e033fc37-c000.csv'
RICHEST_HOLDERS_URL= 'https://storage.googleapis.com/bda_eth/outputs/balances/richest_holders/part-00000-b7c92f66-3741-4564-80ed-aaa600b86dc2-c000.csv'

try:
    api_key = environ["CM_API_KEY"]
    print("Using API key found in environment")
except KeyError:
    api_key = ""
    print("API key not found. Using community client")

@st.cache
def load_erc20_data():
    data = pd.read_csv(CONTRACTS_ERC20_BY_MONTH_URL)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

@st.cache
def load_erc721_data():
    data = pd.read_csv(CONTRACTS_ERC721_BY_MONTH_URL)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

@st.cache
def load_txns_data():
    data = pd.read_csv(TXNS_BY_MONTH_AND_TYPE_URL)  
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

# @st.cache
def load_richest_holders_data():
    data = pd.read_csv(RICHEST_HOLDERS_URL)
    return data

data_load_state = st.text('Loading data...')
erc20_data = load_erc20_data()
erc721_data = load_erc721_data()
txns = load_txns_data()
richest_holders_data = load_richest_holders_data()

data_load_state.text("Done! (using st.cache)")

year_to_filter = st.slider('Year', 2015, 2022, 2021)

erc20_data = erc20_data.set_index('timestamp')
filtered_data_tokens_deployed = erc20_data[erc20_data.index.year == year_to_filter]

erc721_data = erc721_data.set_index('timestamp')
filtered_data_nfts_deployed = erc721_data[erc721_data.index.year == year_to_filter]

st.subheader('Number of transactions by month')
txns = txns.fillna(0)
filtered_txns = txns.set_index('timestamp')
txns = pd.pivot_table(filtered_txns, index=['timestamp'], columns=["transaction_type"], values='transactionsCount')
txns = txns.fillna(0)
txns.columns=['Token Transfers', 'Smart Contracts Deployed', 'Contracts Called']
token_transfers = txns.drop(['Smart Contracts Deployed', 'Contracts Called'], axis=1)
contracts_deployed = txns.drop(['Contracts Called', 'Token Transfers'], axis=1)
contracts_called = txns.drop(['Smart Contracts Deployed', 'Token Transfers'], axis=1)

filtered_token_transfers = token_transfers[token_transfers.index.year == year_to_filter]
filtered_contracts_deployed = contracts_deployed[contracts_deployed.index.year == year_to_filter]
filtered_contracts_called = contracts_called[contracts_called.index.year == year_to_filter]

#Coinmetrics data
client = CoinMetricsClient(api_key)

trades = client.get_market_candles(
    markets='coinbase-eth-usd-spot', 
    start_time = str(year_to_filter) + "-01-01",
    end_time = str(year_to_filter) + "-12-21",
    frequency="1d"
)

trades_df = trades.to_dataframe()

trades_df_open = trades_df[['price_open', 'price_close', 'time']]
trades_df_high_low = trades_df[['price_high', 'price_low', 'time']]
trades_df_trades_per_day = trades_df[[ 'candle_trades_count', 'time']]
trades_df_volume = trades_df[[ 'volume', 'time']]
trades_df_volume_usd = trades_df[[ 'candle_usd_volume', 'time']]

trades_df_open['time'] = pd.to_datetime(trades_df_open['time'])
trades_df_open = trades_df_open.set_index('time')

trades_df_trades_per_day['time'] = pd.to_datetime(trades_df_trades_per_day['time'])
trades_df_trades_per_day = trades_df_trades_per_day.set_index('time')

trades_df_high_low['time'] = pd.to_datetime(trades_df_high_low['time'])
trades_df_high_low = trades_df_high_low.set_index('time')

trades_df_volume['time'] = pd.to_datetime(trades_df_volume['time'])
trades_df_volume = trades_df_volume.set_index('time')

trades_df_volume_usd['time'] = pd.to_datetime(trades_df_volume_usd['time'])
trades_df_volume_usd = trades_df_volume_usd.set_index('time')

col1, col2 = st.columns(2)

with col1:
    st.subheader('Number of tokens deployed by month')
    st.line_chart(filtered_data_tokens_deployed, use_container_width=True)

    st.subheader('Number of NFTs deployed by month')
    st.line_chart(filtered_data_nfts_deployed)

    st.header('Token Transfers')
    st.line_chart(filtered_token_transfers)


    st.header('Smart Contracts Deployed')
    st.line_chart(filtered_contracts_deployed)


    st.header('Smart Contracts Called')
    st.line_chart(filtered_contracts_called)

with col2:
    st.header('Opening Price of ETH')
    st.line_chart(trades_df_open)

    st.header('Low & High Prices of ETH')
    st.line_chart(trades_df_high_low)

    st.header('Trade Volume per day in ETH')
    st.line_chart(trades_df_volume)

    st.header('Trade Volume per day in USD')
    st.line_chart(trades_df_volume_usd)
    
    st.header('Number of Trades Per Day')
    st.line_chart(trades_df_trades_per_day)


st.markdown("""<hr style="height:10px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

st.title('Ethereum Daily Metrics')

col1, col2 = st.columns(2)

with col1:
    richest_holders_data['eth_balance'] = (richest_holders_data['eth_balance'].astype(float   ).div(pow(10,18)))
    st.metric("Richest Address - " + richest_holders_data.address[0], str(richest_holders_data['eth_balance'][0]) + " ETH", delta=None, delta_color="normal")

    st.header('Richest addresses')
    st.write(richest_holders_data, height=500)


with col2: 
    exchange = st.radio(
        "Pick an exchange",
        ('Binance', 'FTX'))

    if exchange == 'FTX':
        markets_funding='ftx-ETH*future'
        markets_liq='ftx-ETH-0930-future'
    elif exchange == 'Binance':
        markets_funding='binance-ETH*future'
        markets_liq='binance-ETHBUSD-future'


    ftx_quotes = client.get_market_funding_rates(
        markets=markets_funding,
        limit_per_market = 100
    ).to_dataframe()

    ftx_quotes = ftx_quotes[['rate', 'time']]
    ftx_quotes['time'] = pd.to_datetime(ftx_quotes['time'])
    ftx_quotes = ftx_quotes.set_index('time')

    liq = client.get_market_open_interest(
        markets=markets_liq 
    ).to_dataframe()

    liq = liq[['value_usd', 'time']]
    liq['time'] = pd.to_datetime(liq['time'])
    liq = liq.set_index('time')
    
    st.header('Funding Rates')
    st.line_chart(ftx_quotes)

    st.header('Open Interest')
    st.line_chart(liq)
