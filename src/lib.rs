#[macro_use]
extern crate derive_builder;

mod fetchers;
mod types;
mod client;

pub use types::{FastNearConfig, FastNearConfig2, FastNearConfigBuilder, FastNearError};
use crate::types::Config;

pub fn streamer(
    config: &dyn Config,
) -> (
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    tokio::sync::mpsc::Receiver<near_indexer_primitives::StreamerMessage>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    if let Some(conf) = config.as_any().downcast_ref::<FastNearConfig>() {
        (tokio::spawn(start(sender, conf.clone())), receiver)
    } else if let Some(conf) = config.as_any().downcast_ref::<FastNearConfig2>() {
        (tokio::spawn(start2(sender, conf.clone())), receiver)
    } else {
        panic!("Invalid config type");
    }
}

const LOG_TARGET: &str = "fastnear-neardata";

pub async fn start2(
    _blocks_sink: tokio::sync::mpsc::Sender<near_indexer_primitives::StreamerMessage>,
    _config: FastNearConfig2,
) -> anyhow::Result<()> {
    todo!("Implement me")
}

pub async fn start(
    blocks_sink: tokio::sync::mpsc::Sender<near_indexer_primitives::StreamerMessage>,
    config: FastNearConfig,
) -> anyhow::Result<()> {
    let client = client::FastNearClient::new(&config);
    let max_num_threads = config.num_threads;
    let next_sink_block =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(config.start_block_height));
    loop {
        // In the beginning of the 'main' loop, we fetch the next block height to start fetching from
        let start_block_height = next_sink_block.load(std::sync::atomic::Ordering::SeqCst);
        let next_fetch_block =
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(start_block_height));
        let last_block_height = fetchers::fetch_last_block(&client)
            .await
            .block
            .header
            .height;
        let is_backfill = last_block_height > start_block_height + max_num_threads;
        let num_threads = if is_backfill { max_num_threads } else { 1 };
        tracing::info!(
            target: LOG_TARGET,
            "Start fetching from block {} to block {} with {} threads. Backfill: {:?}",
            start_block_height,
            last_block_height,
            num_threads,
            is_backfill
        );
        // starting backfill with multiple threads
        let handles = (0..num_threads)
            .map(|thread_index| {
                let client = client.clone();
                let blocks_sink = blocks_sink.clone();
                let next_fetch_block = next_fetch_block.clone();
                let next_sink_block = next_sink_block.clone();
                tokio::spawn(async move {
                    'stream: loop {
                        let block_height = next_fetch_block.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if is_backfill && block_height > last_block_height {
                            break 'stream;
                        }
                        tracing::debug!(target: LOG_TARGET, "#{}: Fetching block: {}", thread_index, block_height);
                        let block =
                            fetchers::fetch_streamer_message(&client, block_height).await;
                        'sender: loop {
                            let expected_block_height = next_sink_block.load(std::sync::atomic::Ordering::SeqCst);
                            if expected_block_height < block_height {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    block_height - expected_block_height,
                                )).await;
                            } else {
                                tracing::debug!(target: LOG_TARGET, "#{}: Sending block: {}", thread_index, block_height);
                                break 'sender;
                            }
                        }
                        if let Some(block) = block {
                            blocks_sink.send(block).await.expect("Failed to send block");
                        } else {
                            tracing::debug!(target: LOG_TARGET, "#{}: Skipped block: {}", thread_index, block_height);
                        }
                        next_sink_block.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await?;
        }
    }
}
