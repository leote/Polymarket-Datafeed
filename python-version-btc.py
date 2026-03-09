#!/usr/bin/env python3
"""Polymarket BTC UP/DOWN 15m WebSocket Feed"""

import asyncio
import json
import time
from datetime import datetime

import aiohttp
import websockets

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com/markets"


class MarketState:
    def __init__(self, up_token: str, down_token: str):
        self.up_token = up_token
        self.down_token = down_token
        self.up_bid = self.up_ask = 0.0
        self.down_bid = self.down_ask = 0.0
        self.up_bids = []
        self.up_asks = []
        self.down_bids = []
        self.down_asks = []

    def _parse_levels(self, data: list) -> list:
        levels = []
        for entry in data:
            try:
                levels.append((float(entry["price"]), float(entry["size"])))
            except (KeyError, ValueError):
                pass
        return levels

    def update_book(self, is_up: bool, bids: list, asks: list):
        if is_up:
            if bids: self.up_bids = sorted(bids, key=lambda x: x[0], reverse=True)
            if asks: self.up_asks = sorted(asks, key=lambda x: x[0])
            self.up_bid = self.up_bids[0][0] if self.up_bids else 0.0
            self.up_ask = self.up_asks[0][0] if self.up_asks else 0.0
        else:
            if bids: self.down_bids = sorted(bids, key=lambda x: x[0], reverse=True)
            if asks: self.down_asks = sorted(asks, key=lambda x: x[0])
            self.down_bid = self.down_bids[0][0] if self.down_bids else 0.0
            self.down_ask = self.down_asks[0][0] if self.down_asks else 0.0

    def handle_message(self, text: str):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return

        events = payload if isinstance(payload, list) else [payload]
        updated = False

        for data in events:
            if not isinstance(data, dict):
                continue
            asset_id = data.get("asset_id", "")
            is_up = asset_id == self.up_token
            is_down = asset_id == self.down_token
            if not is_up and not is_down:
                continue

            if data.get("event_type") == "book":
                bids = self._parse_levels(data.get("bids", []))
                asks = self._parse_levels(data.get("asks", []))
                self.update_book(is_up, bids, asks)
                updated = True

        if updated:
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            combined = self.up_ask + self.down_ask
            print(
                f"[{ts}] UP: {self.up_bid:.4f}/{self.up_ask:.4f} "
                f"(Spr:{self.up_ask - self.up_bid:.4f}) | "
                f"DOWN: {self.down_bid:.4f}/{self.down_ask:.4f} "
                f"(Spr:{self.down_ask - self.down_bid:.4f}) | "
                f"Combined Ask: {combined:.4f}"
            )


class PolymarketFeed:
    def __init__(self):
        self._cached: tuple | None = None
        self._cached_window: int = 0

    async def get_market(self) -> tuple | None:
        now = int(time.time())
        window = (now // 900) * 900
        if self._cached and self._cached_window == window:
            return self._cached

        slug = f"btc-updown-15m-{window}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{GAMMA_API}?slug={slug}", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    text = await resp.text()
                    markets = json.loads(text)
                    if not markets:
                        return None
                    market = markets[0]
                    if not market.get("active"):
                        return None
                    token_ids = json.loads(market["clobTokenIds"])
                    if len(token_ids) < 2:
                        return None
                    print(f"\nMarket: {market['question']}")
                    self._cached = (token_ids[0], token_ids[1])
                    self._cached_window = window
                    return self._cached
        except Exception as e:
            print(f"Error fetching market: {e}")
            return None

    async def run(self):
        print("POLYMARKET BTC FEED — Press Ctrl+C to stop\n")
        while True:
            try:
                result = await self.get_market()
                if not result:
                    await asyncio.sleep(1)
                    continue

                up_token, down_token = result
                async with websockets.connect(WS_URL) as ws:
                    await ws.send(json.dumps({"assets_ids": [up_token, down_token], "type": "market"}))
                    state = MarketState(up_token, down_token)
                    async for message in ws:
                        if isinstance(message, str):
                            state.handle_message(message)

            except (KeyboardInterrupt, asyncio.CancelledError):
                print("\nStopped.")
                return
            except Exception:
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(PolymarketFeed().run())