import websocket


class BinanceWebSocket:
    def __init__(self, symbol, interval, callback):
        self.symbol = symbol
        self.interval = interval
        self.callback = callback
        self.ws = None

    def start(self):
        self.ws = websocket.WebSocketApp(
            f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@kline_{self.interval}",
            on_message=self.on_message,
        )
        self.ws.run_forever()

    def on_message(self, ws, message):
        self.callback(message)
