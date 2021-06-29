###############################################################
####                                                       ####
#### Written By: Satyaki De                                ####
#### Written Date: 26-Jun-2021                             ####
####                                                       ####
#### Objective: This script will consume real-time         ####
#### streaming data coming out from a hosted API           ####
#### sources (Finnhub) using another popular third-party   ####
#### service named Ably. Ably mimics pubsub Streaming      ####
#### concept, which might be extremely useful for          ####
#### any start-ups.                                        ####
####                                                       ####
###############################################################

from ably import AblyRest
import logging
import json

# generate random floating point values
from random import seed
from random import random
# seed random number generator

import websocket
import json

from clsConfig import clsConfig as cf

seed(1)

# Global Section

logger = logging.getLogger('ably')
logger.addHandler(logging.StreamHandler())

ably_id = str(cf.config['ABLY_ID'])

ably = AblyRest(ably_id)
channel = ably.channels.get('sd_channel')

# End Of Global Section

def on_message(ws, message):

    print("*" * 60)
    res = json.loads(message)
    jsBody = res["data"]
    jdata_dyn = json.dumps(jsBody)
    print(jdata_dyn)

    # JSON data
    # This is the default data for all the identified category
    # we've prepared. You can extract this dynamically. Or, By
    # default you can set their base trade details.

    json_data = [{
        "c": "null",
        "p": 0.01,
        "s": "AAPL",
        "t": 1624715406407,
        "v": 0.01
    },{
        "c": "null",
        "p": 0.01,
        "s": "AMZN",
        "t": 1624715406408,
        "v": 0.01
    },{
        "c": "null",
        "p": 0.01,
        "s": "BINANCE:BTCUSDT",
        "t": 1624715406409,
        "v": 0.01
    },
        {
        "c": "null",
        "p": 0.01,
        "s": "IC MARKETS:1",
        "t": 1624715406410,
        "v": 0.01
        }]

    jdata = json.dumps(json_data)

    # Publish a message to the sd_channel channel
    channel.publish('event', jdata)

    # Publish rest of the messages to the sd_channel channel
    channel.publish('event', jdata_dyn)

    jsBody = []
    jdata_dyn = ''

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    # Invoking Individual Company Trade Queries
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=jfhfyr8474rpv6av0",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
