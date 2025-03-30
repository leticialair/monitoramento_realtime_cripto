import websocket
import threading
import json


class BinanceWebSocket:
    def __init__(self, symbol, interval, callback):
        self.symbol = symbol
        self.interval = interval
        self.callback = callback
        self.ws = None

    def start(self):
        endpoint = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@kline_{self.interval}"
        self.ws = websocket.WebSocketApp(
            endpoint,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        threading.Thread(target=self.ws.run_forever).start()

    def on_message(self, ws, message):
        self.callback(message)

    def on_error(self, ws, error):
        print("Erro no WebSocket:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket fechado.")
