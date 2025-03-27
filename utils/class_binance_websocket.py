import websocket
import threading
import json


class BinanceWebSocket:
    """
    Classe para interações com o WebSocket da Binance.
    """

    def __init__(self, symbol, interval, callback):
        self.symbol = symbol
        self.interval = interval
        self.callback = callback
        self.ws = None
        self.thread = None

    def on_message(self, ws, message):
        """Processa a mensagem recebida e chama o callback."""
        data = json.loads(message)
        self.callback(data)

    def on_error(self, ws, error):
        """Lida com erros de conexão."""
        print(f"Erro no WebSocket: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Executado quando a conexão é fechada."""
        print(f"Conexão fechada: {close_status_code} - {close_msg}")

    def on_open(self, ws):
        """Executado ao abrir a conexão."""
        print("Conexão WebSocket aberta.")

    def start(self):
        """Inicia o WebSocket em uma thread separada."""
        url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@kline_{self.interval}"
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

        self.thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.thread.start()

    def stop(self):
        """Fecha a conexão do WebSocket."""
        if self.ws:
            self.ws.close()
