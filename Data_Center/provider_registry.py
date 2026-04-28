from __future__ import annotations

from Data_Center.models import NewsProvider, PriceProvider
from Data_Center.providers.Provider_Alpha_Vantage import NEWS_PROVIDER as ALPHA_NEWS_PROVIDER
from Data_Center.providers.Provider_Alpha_Vantage import PRICE_PROVIDER as ALPHA_PRICE_PROVIDER
from Data_Center.providers.Provider_Finnhub import NEWS_PROVIDER as FINNHUB_NEWS_PROVIDER
from Data_Center.providers.Provider_Finnhub import PRICE_PROVIDER as FINNHUB_PRICE_PROVIDER
from Data_Center.providers.Provider_Marketaux import NEWS_PROVIDER as MARKETAUX_NEWS_PROVIDER
from Data_Center.providers.Provider_NewsAPI import NEWS_PROVIDER as NEWSAPI_NEWS_PROVIDER
from Data_Center.providers.Provider_Polygon import PRICE_PROVIDER as POLYGON_PRICE_PROVIDER
from Data_Center.providers.Provider_Twelve_Data import PRICE_PROVIDER as TWELVE_PRICE_PROVIDER


PRICE_PROVIDERS: dict[str, PriceProvider] = {
    provider.name: provider
    for provider in [
        ALPHA_PRICE_PROVIDER,
        TWELVE_PRICE_PROVIDER,
        FINNHUB_PRICE_PROVIDER,
        POLYGON_PRICE_PROVIDER,
    ]
}

NEWS_PROVIDERS: dict[str, NewsProvider] = {
    provider.name: provider
    for provider in [
        ALPHA_NEWS_PROVIDER,
        FINNHUB_NEWS_PROVIDER,
        MARKETAUX_NEWS_PROVIDER,
        NEWSAPI_NEWS_PROVIDER,
    ]
}
