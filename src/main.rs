use serde::Deserialize;
use serde_json::{json, Value};
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use futures_util::{StreamExt, SinkExt};
use std::io::{self, Write};

#[derive(Debug, Deserialize)]
struct Market {
    question: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    active: bool,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

struct OrderbookSide {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

struct MarketState {
    up_token: String,
    down_token: String,
    market_end: Option<chrono::DateTime<chrono::Utc>>,
    
    up_orderbook: OrderbookSide,
    down_orderbook: OrderbookSide,
    
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
    
    up_last_trade: f64,
    down_last_trade: f64,
    
    message_count: u64,
    book_updates: u64,
    trade_updates: u64,
    
    last_print: Instant,
    last_update: Instant,
}

impl MarketState {
    fn new(up_token: String, down_token: String, end_date: Option<String>) -> Self {
        let market_end = end_date.and_then(|d| chrono::DateTime::parse_from_rfc3339(&d).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));
        
        Self {
            up_token,
            down_token,
            market_end,
            up_orderbook: OrderbookSide { bids: Vec::new(), asks: Vec::new() },
            down_orderbook: OrderbookSide { bids: Vec::new(), asks: Vec::new() },
            up_bid: 0.0,
            up_ask: 0.0,
            down_bid: 0.0,
            down_ask: 0.0,
            up_last_trade: 0.0,
            down_last_trade: 0.0,
            message_count: 0,
            book_updates: 0,
            trade_updates: 0,
            last_print: Instant::now(),
            last_update: Instant::now(),
        }
    }
    
    fn should_transition(&self) -> bool {
        if let Some(end_time) = self.market_end {
            let now = chrono::Utc::now();
            let time_until_end = end_time.signed_duration_since(now);
            
            time_until_end.num_seconds() < 10
        } else {
            false
        }
    }
    
    fn update_orderbook(&mut self, is_up: bool, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        let book = if is_up { &mut self.up_orderbook } else { &mut self.down_orderbook };
        
        if !bids.is_empty() {
            book.bids = bids;
            book.bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        }
        
        if !asks.is_empty() {
            book.asks = asks;
            book.asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        }
        
        let best_bid = book.bids.first().map(|b| b.0).unwrap_or(0.0);
        let best_ask = book.asks.first().map(|a| a.0).unwrap_or(0.0);
        
        if is_up {
            self.up_bid = best_bid;
            self.up_ask = best_ask;
        } else {
            self.down_bid = best_bid;
            self.down_ask = best_ask;
        }
    }
    
    fn handle_message(&mut self, text: &str) {
        self.message_count += 1;
        self.last_update = Instant::now();
        
        if let Ok(data) = serde_json::from_str::<Value>(text) {
            let event_type = data.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
            let asset_id = data.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
            
            let is_up = asset_id == self.up_token;
            let is_down = asset_id == self.down_token;
            
            if !is_up && !is_down {
                return;
            }
            
            match event_type {
                "book" => {
                    self.book_updates += 1;
                    
                    let bids = data.get("bids")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|b| {
                                    let price = b.get("price")?.as_str()?.parse::<f64>().ok()?;
                                    let size = b.get("size")?.as_str()?.parse::<f64>().ok()?;
                                    Some((price, size))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    
                    let asks = data.get("asks")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|a| {
                                    let price = a.get("price")?.as_str()?.parse::<f64>().ok()?;
                                    let size = a.get("size")?.as_str()?.parse::<f64>().ok()?;
                                    Some((price, size))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    
                    self.update_orderbook(is_up, bids, asks);
                }
                "last_trade_price" => {
                    self.trade_updates += 1;
                    
                    if let Some(price) = data.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                        if is_up {
                            self.up_last_trade = price;
                        } else {
                            self.down_last_trade = price;
                        }
                    }
                }
                _ => {}
            }
            
            if self.last_print.elapsed() >= Duration::from_millis(100) {
                self.print_state();
                self.last_print = Instant::now();
            }
        }
    }
    
    fn print_state(&self) {
        let timestamp = chrono::Local::now().format("%H:%M:%S%.3f");
        
        let combined_ask = self.up_ask + self.down_ask;
        let up_spread = self.up_ask - self.up_bid;
        let down_spread = self.down_ask - self.down_bid;
        
        print!("\r[{}] UP: {:.4}/{:.4} (Spr:{:.4}) | DOWN: {:.4}/{:.4} (Spr:{:.4}) | Combined: {:.4}",
            timestamp,
            self.up_bid, self.up_ask, up_spread,
            self.down_bid, self.down_ask, down_spread,
            combined_ask
        );
        
        // Show countdown if market is ending soon
        if let Some(end_time) = self.market_end {
            let now = chrono::Utc::now();
            let remaining = end_time.signed_duration_since(now);
            if remaining.num_seconds() > 0 && remaining.num_seconds() < 60 {
                print!(" | Ends in {}s", remaining.num_seconds());
            }
        }
        
        let _ = io::stdout().flush();
    }
    
    fn print_stats(&self) {
        println!("\n\nSession Stats:");
        println!("  Total messages: {}", self.message_count);
        println!("  Book updates: {}", self.book_updates);
        println!("  Trade updates: {}", self.trade_updates);
    }
}

struct PolymarketFeed {
    client: reqwest::Client,
}

impl PolymarketFeed {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn get_current_market(&self) -> Result<Option<(String, String, String, Option<String>)>, Box<dyn Error>> {
        let current_time = chrono::Utc::now().timestamp();
        let market_time = (current_time / 900) * 900;

        let slugs = vec![
            format!("btc-updown-15m-{}", market_time),
        ];

        for slug in &slugs {
            let encoded_slug = urlencoding::encode(slug);
            let url = format!("https://gamma-api.polymarket.com/markets?slug={}", encoded_slug);

            match self.client.get(&url).timeout(Duration::from_secs(5)).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        let text = response.text().await?;
                        if text.trim().is_empty() || text == "[]" {
                            continue;
                        }

                        let markets: Vec<Market> = match serde_json::from_str(&text) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };

                        if let Some(market) = markets.into_iter().next() {
                            if !market.active {
                                continue;
                            }

                            let token_ids: Vec<String> = market.clob_token_ids
                                .trim_matches(|c| c == '[' || c == ']' || c == '"')
                                .split(',')
                                .map(|s| s.trim().trim_matches('"').to_string())
                                .collect();

                            if token_ids.len() >= 2 {
                                println!("\nFound market: {}", market.question);
                                if let Some(ref end_date) = market.end_date {
                                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(end_date) {
                                        println!("Market ends: {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
                                    }
                                }
                                println!("UP token: {}", &token_ids[0]);
                                println!("DOWN token: {}", &token_ids[1]);

                                return Ok(Some((
                                    token_ids[0].clone(),
                                    token_ids[1].clone(),
                                    market.question,
                                    market.end_date,
                                )));
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(None)
    }

    async fn connect_websocket(&self, up_token: String, down_token: String, question: String, end_date: Option<String>) -> Result<bool, Box<dyn Error>> {
        println!("Connecting to WebSocket...");
        
        let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
        let (ws_stream, _) = tokio_tungstenite::connect_async(url).await?;
        
        println!("WebSocket connected");
        
        let (mut write, mut read) = ws_stream.split();
        
        let subscription = json!({
            "assets_ids": [&up_token, &down_token],
            "type": "market"
        });
        
        write.send(tokio_tungstenite::tungstenite::Message::Text(subscription.to_string())).await?;
        println!("Subscribed to tokens\n");
        
        let mut state = MarketState::new(up_token, down_token, end_date);
        
        println!("Starting feed for: {}", question);
        println!("{}", "-".repeat(100));
        
        let mut transition_check = tokio::time::interval(Duration::from_secs(1));
        
        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            state.handle_message(&text);
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                            println!("\n\nWebSocket closed by server");
                            state.print_stats();
                            return Ok(false);
                        }
                        Some(Err(e)) => {
                            println!("\n\nWebSocket error: {}", e);
                            state.print_stats();
                            return Err(Box::new(e));
                        }
                        None => {
                            println!("\n\nWebSocket stream ended");
                            state.print_stats();
                            return Ok(false);
                        }
                        _ => {}
                    }
                }
                _ = transition_check.tick() => {
                    if state.should_transition() {
                        println!("\n\nMarket ending in 10 seconds, preparing to transition...");
                        state.print_stats();
                        return Ok(true);
                    }
                }
            }
        }
    }

    async fn run(&self) -> Result<(), Box<dyn Error>> {
        println!("POLYMARKET BTC FEED");
        println!("{}", "-".repeat(100));
        
        loop {
            match self.get_current_market().await {
                Ok(Some((up_token, down_token, question, end_date))) => {
                    match self.connect_websocket(up_token, down_token, question, end_date).await {
                        Ok(should_transition) => {
                            if should_transition {
                                println!("Transitioning to next market...");
                                sleep(Duration::from_secs(5)).await;
                            } else {
                                println!("\nWebSocket disconnected normally");
                                sleep(Duration::from_secs(10)).await;
                            }
                        }
                        Err(e) => {
                            println!("\nWebSocket error: {}", e);
                            sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
                Ok(None) => {
                    println!("\nNo active market found");
                    sleep(Duration::from_secs(10)).await;
                }
                Err(e) => {
                    println!("\nError: {}", e);
                    sleep(Duration::from_secs(10)).await;
                }
            }
            
            println!("Retrying...\n");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let feed = PolymarketFeed::new();
    
    println!("Starting feed: ");
    println!("Press Ctrl+C to stop\n");
    
    if let Err(e) = feed.run().await {
        println!("Fatal error: {}", e);
    }
    
    Ok(())
}
