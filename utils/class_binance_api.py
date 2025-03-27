import pandas as pd
import requests


class BinanceAPI:
    """
    Classe para interações com a API Rest da Binance.
    """

    def __init__(self):
        self.url_base = "https://api.binance.com/api/"

    def get_top20_symbols(self) -> list:
        """
        Seleciona as 20 criptomoedas mais negociadas na Binance.
        """
        url_ticker = self.url_base + "v3/ticker/24hr"
        response = requests.get(url_ticker).json()
        top_symbols = (
            pd.DataFrame(response)
            .sort_values("quoteVolume", ascending=False)
            .head(20)["symbol"]
            .tolist()
        )
        return top_symbols


if __name__ == "__main__":
    api = BinanceAPI()
    print(api.get_top20_symbols())
