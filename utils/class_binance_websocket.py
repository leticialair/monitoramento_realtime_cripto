import websocket
import threading
import json


class BinanceWebSocket:
    def __init__(
        self,
        symbol: str,
        interval: str,
        callback_kline: callable,
        callback_ticker: callable,
        callback_trade: callable,
    ):
        """
        Inicializa o WebSocket da Binance para monitoramento de
        três tipos de dados: Kline, Ticker e Trades e mantém o
        WebSocket rodando em uma thread separada.

        Possui inscrição dinâmica automática nos três streams ao
        abrir a conexão.

        Parâmetros:
            symbol:
                Símbolo da criptomoeda (ex: 'btcusdt')
            interval:
                Intervalo da vela (ex: '1m', '5m')
            callback_kline:
                Função de callback para dados de velas (Kline)
            callback_ticker:
                Função de callback para dados de cotação (Ticker)
            callback_trade:
                Função de callback para dados de negociações individuais (Trade)
        """
        self.symbol = symbol.lower()
        self.interval = interval
        self.callback_kline = callback_kline
        self.callback_ticker = callback_ticker
        self.callback_trade = callback_trade
        self.ws = None
        self.thread = None

    def on_message(self, ws, message):
        """Processa a mensagem recebida e chama o callback apropriado."""
        data = json.loads(message)

        # Verifica o tipo da mensagem recebida e encaminha para o callback correto
        if "k" in data:  # Mensagem de Kline (vela)
            self.callback_kline(data)
        elif (
            "e" in data and data["e"] == "24hrTicker"
        ):  # Mensagem de Ticker (preço atualizado)
            self.callback_ticker(data)
        elif (
            "e" in data and data["e"] == "trade"
        ):  # Mensagem de Trade (transação individual)
            self.callback_trade(data)

    def on_error(self, ws, error):
        """Lida com erros de conexão."""
        print(f"Erro no WebSocket: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Executado quando a conexão é fechada."""
        print(f"Conexão fechada: {close_status_code} - {close_msg}")

    def on_open(self, ws):
        """Executado ao abrir a conexão. Envia a solicitação de inscrição nos streams desejados."""
        print("Conexão WebSocket aberta.")

        # Criando payload para inscrição nos streams de Kline, Ticker e Trades
        params = [
            f"{self.symbol}@kline_{self.interval}",
            f"{self.symbol}@ticker",
            f"{self.symbol}@trade",
        ]

        payload = {"method": "SUBSCRIBE", "params": params, "id": 1}

        ws.send(json.dumps(payload))

    def start(self):
        """Inicia o WebSocket em uma thread separada para não bloquear a execução principal."""
        url = "wss://stream.binance.com:9443/ws"

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


if __name__ == "__main__":
    # Exemplo de callbacks
    def process_kline(data):
        print(f"Kline: {data}")

    def process_ticker(data):
        print(f"Ticker: {data}")

    def process_trade(data):
        print(f"Trade: {data}")

    # Exemplo de uso
    ws = BinanceWebSocket("btcusdt", "1m", process_kline, process_ticker, process_trade)
    ws.start()
