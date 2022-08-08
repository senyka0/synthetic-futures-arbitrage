import requests
import pandas as pd
import datetime
import time
import ccxt
from math import floor
from pytz import UTC
exchange = ccxt.delta()
exchange.apiKey = '' #API key from delta
exchange.secret = '' #Secret key from delta
import telebot
bot = telebot.TeleBot('') #put here your bot token to recieve messeges about trades
k = 0.5  #minimum expected profit for trade
minSum = 50 #minimum trade size    (50*0.5)=0.25$ (average cost of commissions is 0.20$) 
daysToExpi = 3 #maximum days till expiration

while True:
    try:
        values = {i['contract_unit_currency']: float(i['contract_value']) for i in requests.get('https://api.delta.exchange/v2/products?contract_types=perpetual_futures').json()['result']}
        deltaSymb = requests.get('https://p-api.delta.exchange/v2/tickers?contract_types=put_options,call_options,turbo_put_options,turbo_call_options').json()['result']
        deltaData = []
        for i in deltaSymb:
            try:
                deltaData.append({
                    'name': i['symbol'],
                    'type': i['symbol'].split('-')[0],
                    'symbol': i['symbol'].split('-')[1],
                    'strike': float(i['symbol'].split('-')[2]),
                    'expiration': datetime.datetime.strptime(i['symbol'].split('-')[3],'%d%m%y').replace(hour=12).replace(tzinfo=UTC),
                    'ask': float(i['quotes']['best_ask']),
                    'askSize': float(i['quotes']['ask_size']),
                    'bid': float(i['quotes']['best_bid']),
                    'bidSize': float(i['quotes']['bid_size']),
                    'delta': i['greeks']['delta'],
                    'gamma': i['greeks']['gamma'],
                    'theta': i['greeks']['theta'],
                    'vega': i['greeks']['vega']
                })
            except Exception as ew:
                continue
        newDf = pd.DataFrame(deltaData)
        prices = {i['oi_value_symbol']: i['close'] for i in requests.get('https://p-api.delta.exchange/v2/tickers?contract_types=perpetual_futures').json()['result'] if i['turnover_symbol']=='USDT'}
        for symb in list(set(newDf['symbol'])):
            for strik in list(set(newDf[newDf['symbol']==symb]['strike'])):
                for expi in list(set(newDf[(newDf['symbol']==symb)&(newDf['strike']==strik)]['expiration'])):
                    tempDf = newDf[(newDf['symbol']==symb)&(newDf['strike']==strik)&(newDf['expiration']==expi)].sort_values(['type']).reset_index(drop=True)
                    if len(tempDf)==2 and expi < datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=daysToExpi):
                        if prices[symb]<strik:
                            long = strik-(tempDf['bid'][1]-tempDf['ask'][0])
                            short = strik-(tempDf['ask'][1]-tempDf['bid'][0])
                            if (prices[symb]-long)/long*100>k and prices[symb]*(tempDf['bidSize'][1]*values[symb])>minSum/3 and prices[symb]*(tempDf['askSize'][0]*values[symb])>minSum/3:
                                contracts = min(floor(exchange.fetch_balance()['free']['5']/(values[symb]*prices[symb]+tempDf['bid'][1]*values[symb]+tempDf['ask'][0]*values[symb])), tempDf['askSize'][0], tempDf['bidSize'][1])
                                if contracts >= 1:
                                    bid = exchange.fetch_ticker(tempDf['name'][1])['info']['quotes']
                                    ask = exchange.fetch_ticker(tempDf['name'][0])['info']['quotes']
                                    if float(bid['best_bid'])==tempDf['bid'][1] and float(bid['bid_size'])==tempDf['bidSize'][1] and float(ask['best_ask'])==tempDf['ask'][0] and float(ask['ask_size'])==tempDf['askSize'][0]:
                                        exchange.create_market_buy_order(tempDf['name'][0], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(tempDf['name'][1], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(f'{symb}USDT', contracts, params = {'leverage': 1})
                                        bot.send_message(501179740, f'Side: Long\nAsset: {symb}\nPrice: {prices[symb]}\nStrike: {strik}\nExpiration: {expi.strftime("%d/%m/%Y")}\n{tempDf["type"][0]}  ask: {tempDf["ask"][0]}/{tempDf["askSize"][0]}   bid: {tempDf["bid"][0]}/{tempDf["bidSize"][0]}\n{tempDf["type"][1]}  ask: {tempDf["ask"][1]}/{tempDf["askSize"][1]}   bid: {tempDf["bid"][1]}/{tempDf["bidSize"][1]}\n\nProfit: {(prices[symb]-long)/long*100}')
                            elif (short-prices[symb])/prices[symb]*100>k and prices[symb]*(tempDf['askSize'][1]*values[symb])>minSum/3 and prices[symb]*(tempDf['bidSize'][0]*values[symb])>minSum/3:
                                contracts = min(floor(exchange.fetch_balance()['free']['5']/(values[symb]*prices[symb]+tempDf['ask'][1]*values[symb]+tempDf['bid'][0]*values[symb])), tempDf['askSize'][1], tempDf['bidSize'][0])
                                if contracts >= 1:
                                    bid = exchange.fetch_ticker(tempDf['name'][0])['info']['quotes']
                                    ask = exchange.fetch_ticker(tempDf['name'][1])['info']['quotes']
                                    if float(bid['best_bid'])==tempDf['bid'][0] and float(bid['bid_size'])==tempDf['bidSize'][0] and float(ask['best_ask'])==tempDf['ask'][1] and float(ask['ask_size'])==tempDf['askSize'][1]:
                                        exchange.create_market_buy_order(tempDf['name'][1], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(tempDf['name'][0], contracts, params = {'leverage': 1})
                                        exchange.create_market_buy_order(f'{symb}USDT', contracts, params = {'leverage': 1})
                                        bot.send_message(501179740, f'Side: Short\nAsset: {symb}\nPrice: {prices[symb]}\nStrike: {strik}\nExpiration: {expi.strftime("%d/%m/%Y")}\n{tempDf["type"][0]}  ask: {tempDf["ask"][0]}/{tempDf["askSize"][0]}   bid: {tempDf["bid"][0]}/{tempDf["bidSize"][0]}\n{tempDf["type"][1]}  ask: {tempDf["ask"][1]}/{tempDf["askSize"][1]}   bid: {tempDf["bid"][1]}/{tempDf["bidSize"][1]}\n\nProfit: {(short-prices[symb])/prices[symb]*100}')
                        elif prices[symb]>strik:
                            long = strik+(tempDf['ask'][0]-tempDf['bid'][1])
                            short = strik+(tempDf['bid'][0]-tempDf['ask'][1])
                            if (prices[symb]-long)/long*100>k and prices[symb]*(tempDf['bidSize'][1]*values[symb])>minSum/3 and prices[symb]*(tempDf['askSize'][0]*values[symb])>minSum/3:
                                contracts = min(floor(exchange.fetch_balance()['free']['5']/(values[symb]*prices[symb]+tempDf['bid'][1]*values[symb]+tempDf['ask'][0]*values[symb])), tempDf['askSize'][0], tempDf['bidSize'][1])
                                if contracts >= 1:
                                    bid = exchange.fetch_ticker(tempDf['name'][1])['info']['quotes']
                                    ask = exchange.fetch_ticker(tempDf['name'][0])['info']['quotes']
                                    if float(bid['best_bid'])==tempDf['bid'][1] and float(bid['bid_size'])==tempDf['bidSize'][1] and float(ask['best_ask'])==tempDf['ask'][0] and float(ask['ask_size'])==tempDf['askSize'][0]:
                                        exchange.create_market_buy_order(tempDf['name'][0], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(tempDf['name'][1], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(f'{symb}USDT', contracts, params = {'leverage': 1})
                                        bot.send_message(501179740, f'Side: Long\nAsset: {symb}\nPrice: {prices[symb]}\nStrike: {strik}\nExpiration: {expi.strftime("%d/%m/%Y")}\n{tempDf["type"][0]}  ask: {tempDf["ask"][0]}/{tempDf["askSize"][0]}   bid: {tempDf["bid"][0]}/{tempDf["bidSize"][0]}\n{tempDf["type"][1]}  ask: {tempDf["ask"][1]}/{tempDf["askSize"][1]}   bid: {tempDf["bid"][1]}/{tempDf["bidSize"][1]}\n\nProfit: {(prices[symb]-long)/long*100}')
                            elif (short-prices[symb])/prices[symb]*100>k and prices[symb]*(tempDf['askSize'][1]*values[symb])>minSum/3 and prices[symb]*(tempDf['bidSize'][0]*values[symb])>minSum/3:
                                contracts = min(floor(exchange.fetch_balance()['free']['5']/(values[symb]*prices[symb]+tempDf['ask'][1]*values[symb]+tempDf['bid'][0]*values[symb])), tempDf['askSize'][1], tempDf['bidSize'][0])
                                if contracts >= 1:
                                    bid = exchange.fetch_ticker(tempDf['name'][0])['info']['quotes']
                                    ask = exchange.fetch_ticker(tempDf['name'][1])['info']['quotes']
                                    if float(bid['best_bid'])==tempDf['bid'][0] and float(bid['bid_size'])==tempDf['bidSize'][0] and float(ask['best_ask'])==tempDf['ask'][1] and float(ask['ask_size'])==tempDf['askSize'][1]:
                                        exchange.create_market_buy_order(tempDf['name'][1], contracts, params = {'leverage': 1})
                                        exchange.create_market_sell_order(tempDf['name'][0], contracts, params = {'leverage': 1})
                                        exchange.create_market_buy_order(f'{symb}USDT', contracts, params = {'leverage': 1})
                                        bot.send_message(501179740, f'Side: Short\nAsset: {symb}\nPrice: {prices[symb]}\nStrike: {strik}\nExpiration: {expi.strftime("%d/%m/%Y")}\n{tempDf["type"][0]}  ask: {tempDf["ask"][0]}/{tempDf["askSize"][0]}   bid: {tempDf["bid"][0]}/{tempDf["bidSize"][0]}\n{tempDf["type"][1]}  ask: {tempDf["ask"][1]}/{tempDf["askSize"][1]}   bid: {tempDf["bid"][1]}/{tempDf["bidSize"][1]}\n\nProfit: {(short-prices[symb])/prices[symb]*100}')
    except Exception as e:
        print(e)
        time.sleep(5)
        continue
