use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use futures_util::{StreamExt, SinkExt};
use std::io::{self, Write};

const MARKET_SLUG_PREFIX: &str = "btc-updown-15m"; // e.g. "btc-updown-5m", "eth-updown-15m"
const BOOK_CAPACITY: usize = 64;

fn interval_secs(prefix: &str) -> i64 {
    prefix.rsplit('-').next()
        .and_then(|s| {
            if let Some(n) = s.strip_suffix('m') {
                n.parse::<i64>().ok().map(|m| m * 60)
            } else if let Some(n) = s.strip_suffix('h') {
                n.parse::<i64>().ok().map(|h| h * 3600)
            } else {
                None
            }
        })
        .unwrap_or(900)
}

// ── REST types ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Market {
    question: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    active: bool,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

// ── WebSocket types (zero-copy borrowed from the raw message) ────────────────

#[derive(Deserialize)]
struct Level<'a> {
    #[serde(borrow)] price: &'a str,
    #[serde(borrow)] size:  &'a str,
}

#[derive(Deserialize)]
struct WsEvent<'a> {
    #[serde(borrow)] event_type: &'a str,
    #[serde(borrow)] asset_id:   &'a str,
    bids:  Option<Vec<Level<'a>>>,
    asks:  Option<Vec<Level<'a>>>,
    #[serde(borrow)] price: Option<&'a str>,
}

// ── Orderbook ────────────────────────────────────────────────────────────────

struct OrderbookSide {
    bids: Vec<(f64, f64)>, // price, size — sorted best-first
    asks: Vec<(f64, f64)>,
}

impl OrderbookSide {
    fn new() -> Self {
        Self {
            bids: Vec::with_capacity(BOOK_CAPACITY),
            asks: Vec::with_capacity(BOOK_CAPACITY),
        }
    }

    #[inline(always)]
    fn replace_bids(&mut self, levels: Vec<Level>) {
        self.bids.clear();
        self.bids.extend(levels.iter().filter_map(parse_level));
        self.bids.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    #[inline(always)]
    fn replace_asks(&mut self, levels: Vec<Level>) {
        self.asks.clear();
        self.asks.extend(levels.iter().filter_map(parse_level));
        self.asks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    #[inline(always)] fn best_bid(&self) -> f64 { self.bids.first().map_or(0.0, |b| b.0) }
    #[inline(always)] fn best_ask(&self) -> f64 { self.asks.first().map_or(0.0, |a| a.0) }
}

#[inline(always)]
fn parse_level(l: &Level) -> Option<(f64, f64)> {
    Some((l.price.parse::<f64>().ok()?, l.size.parse::<f64>().ok()?))
}

// ── Market state ─────────────────────────────────────────────────────────────

struct MarketState {
    up_token:   String,
    down_token: String,
    market_end: Option<chrono::DateTime<chrono::Utc>>,

    up_book:   OrderbookSide,
    down_book: OrderbookSide,

    up_bid:  f64,
    up_ask:  f64,
    down_bid: f64,
    down_ask: f64,

    up_last_trade:   f64,
    down_last_trade: f64,

    message_count: u64,
    book_updates:  u64,
    trade_updates: u64,

    last_print: Instant,
}

impl MarketState {
    fn new(up_token: String, down_token: String, end_date: Option<String>) -> Self {
        let market_end = end_date
            .and_then(|d| chrono::DateTime::parse_from_rfc3339(&d).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));
        Self {
            up_token, down_token, market_end,
            up_book:   OrderbookSide::new(),
            down_book: OrderbookSide::new(),
            up_bid: 0.0, up_ask: 0.0, down_bid: 0.0, down_ask: 0.0,
            up_last_trade: 0.0, down_last_trade: 0.0,
            message_count: 0, book_updates: 0, trade_updates: 0,
            last_print: Instant::now(),
        }
    }

    #[inline(always)]
    fn should_transition(&self) -> bool {
        self.market_end.map_or(false, |end| {
            end.signed_duration_since(chrono::Utc::now()).num_seconds() < 10
        })
    }

    #[inline(always)]
    fn handle_message(&mut self, text: &str) {
        self.message_count += 1;

        let Ok(event) = serde_json::from_str::<WsEvent>(text) else { return };

        let is_up = event.asset_id == self.up_token;
        if !is_up && event.asset_id != self.down_token { return; }

        match event.event_type {
            "book" => {
                self.book_updates += 1;
                let book = if is_up { &mut self.up_book } else { &mut self.down_book };

                if let Some(bids) = event.bids { book.replace_bids(bids); }
                if let Some(asks) = event.asks { book.replace_asks(asks); }

                let (bid, ask) = (book.best_bid(), book.best_ask());
                if is_up { self.up_bid = bid; self.up_ask = ask; }
                else      { self.down_bid = bid; self.down_ask = ask; }
            }
            "last_trade_price" => {
                self.trade_updates += 1;
                if let Some(p) = event.price.and_then(|s| s.parse::<f64>().ok()) {
                    if is_up { self.up_last_trade = p; } else { self.down_last_trade = p; }
                }
            }
            _ => return,
        }

        if self.last_print.elapsed() >= Duration::from_millis(100) {
            self.print_state();
            self.last_print = Instant::now();
        }
    }

    fn print_state(&self) {
        let ts = chrono::Local::now().format("%H:%M:%S%.3f");
        print!("\r[{}] UP: {:.4}/{:.4} Spr:{:.4} | DOWN: {:.4}/{:.4} Spr:{:.4}",
            ts,
            self.up_bid,   self.up_ask,   self.up_ask   - self.up_bid,
            self.down_bid, self.down_ask, self.down_ask - self.down_bid,
        );
        if let Some(end) = self.market_end {
            let s = end.signed_duration_since(chrono::Utc::now()).num_seconds();
            if s > 0 && s < 60 { print!(" | {}s", s); }
        }
        let _ = io::stdout().flush();
    }

    fn print_stats(&self) {
        println!("\n\nStats — msg:{} book:{} trade:{}",
            self.message_count, self.book_updates, self.trade_updates);
    }
}

// ── Feed ─────────────────────────────────────────────────────────────────────

struct PolymarketFeed {
    client:   reqwest::Client,
    interval: i64,
}

impl PolymarketFeed {
    fn new() -> Self {
        Self {
            client:   reqwest::Client::new(),
            interval: interval_secs(MARKET_SLUG_PREFIX),
        }
    }

    async fn get_current_market(&self) -> Result<Option<(String, String, String, Option<String>)>, Box<dyn Error>> {
        let now = chrono::Utc::now().timestamp();
        let market_time = (now / self.interval) * self.interval;

        let slug = format!("{}-{}", MARKET_SLUG_PREFIX, market_time);
        let url  = format!("https://gamma-api.polymarket.com/markets?slug={}", urlencoding::encode(&slug));

        let Ok(resp) = self.client.get(&url).timeout(Duration::from_secs(5)).send().await else {
            return Ok(None);
        };
        if !resp.status().is_success() { return Ok(None); }

        let text = resp.text().await?;
        if text.trim().is_empty() || text == "[]" { return Ok(None); }

        let Ok(markets) = serde_json::from_str::<Vec<Market>>(&text) else { return Ok(None) };
        let Some(market) = markets.into_iter().next() else { return Ok(None) };
        if !market.active { return Ok(None); }

        let token_ids: Vec<String> = market.clob_token_ids
            .trim_matches(|c| c == '[' || c == ']' || c == '"')
            .split(',')
            .map(|s| s.trim().trim_matches('"').to_string())
            .collect();

        if token_ids.len() < 2 { return Ok(None); }

        println!("\nFound: {}", market.question);
        if let Some(ref end) = market.end_date {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(end) {
                println!("Ends:  {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
            }
        }
        println!("UP:   {}\nDOWN: {}", token_ids[0], token_ids[1]);

        Ok(Some((token_ids[0].clone(), token_ids[1].clone(), market.question, market.end_date)))
    }

    async fn connect_websocket(
        &self,
        up_token: String, down_token: String,
        question: String, end_date: Option<String>,
    ) -> Result<bool, Box<dyn Error>> {
        println!("Connecting...");
        let (ws_stream, _) = tokio_tungstenite::connect_async(
            "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        ).await?;

        let (mut write, mut read) = ws_stream.split();
        write.send(tokio_tungstenite::tungstenite::Message::Text(
            json!({ "assets_ids": [&up_token, &down_token], "type": "market" }).to_string()
        )).await?;

        println!("Subscribed — {}\n{}", question, "-".repeat(80));

        let mut state = MarketState::new(up_token, down_token, end_date);
        let mut ticker = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                msg = read.next() => match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        state.handle_message(&text);
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => {
                        println!("\nClosed");
                        state.print_stats();
                        return Ok(false);
                    }
                    Some(Err(e)) => {
                        println!("\nError: {}", e);
                        state.print_stats();
                        return Err(Box::new(e));
                    }
                    _ => {}
                },
                _ = ticker.tick() => {
                    if state.should_transition() {
                        println!("\nTransitioning...");
                        state.print_stats();
                        return Ok(true);
                    }
                }
            }
        }
    }

    async fn run(&self) -> Result<(), Box<dyn Error>> {
        println!("POLYMARKET  {}\n{}", MARKET_SLUG_PREFIX, "-".repeat(80));
        loop {
            match self.get_current_market().await {
                Ok(Some((up, down, q, end))) => {
                    match self.connect_websocket(up, down, q, end).await {
                        Ok(true)  => sleep(Duration::from_secs(5)).await,
                        Ok(false) => sleep(Duration::from_secs(10)).await,
                        Err(e)    => { println!("\nWS error: {}", e); sleep(Duration::from_secs(10)).await; }
                    }
                }
                Ok(None) => { println!("No active market, retrying..."); sleep(Duration::from_secs(10)).await; }
                Err(e)   => { println!("Error: {}", e);                  sleep(Duration::from_secs(10)).await; }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("{} — Ctrl+C to stop\n", MARKET_SLUG_PREFIX);
    PolymarketFeed::new().run().await
}
