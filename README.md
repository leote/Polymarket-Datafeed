# Polymarket-Datafeed
rust datafeed for polymarket




discovers the current 15m market via the Gamma REST API, then subscribes to the CLOB WebSocket and prints live best bid/ask, spreads, and combined ask for the UP and DOWN tokens.

appends market data to a file called market_data.csv
