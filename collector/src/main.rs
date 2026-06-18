use anyhow::anyhow;
use clap::Parser;
use tokio::{self, select, signal, sync::mpsc::unbounded_channel};
use tracing::{error, info};

use crate::file::Writer;

mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod error;
mod file;
mod hyperliquid;
mod throttler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,

    /// Subscriptions to collect. If omitted, use exchange defaults.
    ///
    /// For Binance:
    /// - $symbol@trade
    /// - $symbol@bookTicker
    /// - $symbol@depth@0ms (futures)
    /// - $symbol@depth@100ms (spot)
    ///
    /// For Bybit:
    /// - orderbook.1.$symbol
    /// - orderbook.50.$symbol
    /// - orderbook.500.$symbol
    /// - publicTrade.$symbol
    ///
    /// For Hyperliquid:
    /// - trades
    /// - l2Book
    /// - bbo
    ///
    /// Example:
    /// collector data/raw binancefuturesum BTCUSDT --subscriptions '$symbol@bookTicker'
    #[arg(long = "subscriptions", short = 's')]
    subscriptions: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let (writer_tx, mut writer_rx) = unbounded_channel();

    let handle = match args.exchange.as_str() {
        "binancefutures" | "binancefuturesum" => {
            let streams = if args.subscriptions.is_empty() {
                [
                    "$symbol@trade",
                    "$symbol@bookTicker",
                    "$symbol@depth@0ms",
                    // "$symbol@@markPrice@1s"
                ]
                .iter()
                .map(|stream| stream.to_string())
                .collect()
            } else {
                args.subscriptions.clone()
            };

            tokio::spawn(binancefuturesum::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binancefuturescm" => {
            let streams = if args.subscriptions.is_empty() {
                [
                    "$symbol@trade",
                    "$symbol@bookTicker",
                    "$symbol@depth@0ms",
                    // "$symbol@@markPrice@1s"
                ]
                .iter()
                .map(|stream| stream.to_string())
                .collect()
            } else {
                args.subscriptions.clone()
            };

            tokio::spawn(binancefuturescm::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binance" | "binancespot" => {
            let streams = if args.subscriptions.is_empty() {
                ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                    .iter()
                    .map(|stream| stream.to_string())
                    .collect()
            } else {
                args.subscriptions.clone()
            };

            tokio::spawn(binance::run_collection(streams, args.symbols, writer_tx))
        }
        "bybit" => {
            let topics = if args.subscriptions.is_empty() {
                [
                    "orderbook.1.$symbol",
                    "orderbook.50.$symbol",
                    "orderbook.500.$symbol",
                    "publicTrade.$symbol",
                ]
                .iter()
                .map(|topic| topic.to_string())
                .collect()
            } else {
                args.subscriptions.clone()
            };

            tokio::spawn(bybit::run_collection(topics, args.symbols, writer_tx))
        }
        "hyperliquid" => {
            let subscriptions = if args.subscriptions.is_empty() {
                ["trades", "l2Book", "bbo"]
                    .iter()
                    .map(|sub| sub.to_string())
                    .collect()
            } else {
                args.subscriptions.clone()
            };

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    let mut writer = Writer::new(&args.path);
    loop {
        select! {
            _ = signal::ctrl_c() => {
                info!("ctrl-c received");
                break;
            }
            r = writer_rx.recv() => match r {
                Some((recv_time, symbol, data)) => {
                    if let Err(error) = writer.write(recv_time, symbol, data) {
                        error!(?error, "write error");
                        break;
                    }
                }
                None => {
                    break;
                }
            }
        }
    }
    // let _ = handle.await;
    Ok(())
}
