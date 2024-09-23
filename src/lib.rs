#[macro_use]
extern crate derive_builder;

mod fetchers;
mod types;

use crate::fetchers::FastNearClient;
use crate::types::{FastNearData, FastNearError};
use futures::StreamExt;
use tokio::sync::mpsc;
pub use types::{FastNearConfig, FastNearConfigBuilder};

pub fn streamer(
    config: FastNearConfig,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<near_indexer_primitives::StreamerMessage>,
) {
    let (sender, receiver) = mpsc::channel(100);
    (tokio::spawn(start_fetcher(config, sender)), receiver)
}

// /// Spawns a task that sends objects in strict sequential order to the `mpsc` channel
// /// by fetching in concurrent batches and maintaining ordering.
// async fn start(
//     config: FastNearConfig,
//     sender: mpsc::Sender<near_indexer_primitives::StreamerMessage>,
// ) {
//     let fast_near_client = FastNearClient::new(&config);
//     let mut current_block_height = config.start_block_height;
//     let mut start_block_height = config.start_block_height;
//     let mut pending: std::collections::HashMap<
//         types::BlockHeight,
//         tokio::task::JoinHandle<Result<types::FastNearData, types::FastNearError>>,
//     > = std::collections::HashMap::new();
//     loop {
//         // Fill the pending queue with new fetch tasks until the blocks_preload_pool_size
//         while pending.len() < config.blocks_preload_pool_size {
//             let block_height = current_block_height;
//             let fast_near_client_clone = fast_near_client.clone();
//             let task =
//                 tokio::spawn(async move { fast_near_client_clone.get_object(block_height).await });
//             pending.insert(block_height, task);
//             current_block_height += 1;
//         }
//
//         // Process objects in strict order by block_height
//         if let Some(task) = pending.remove(&start_block_height) {
//             match task.await {
//                 Ok(result) => match result {
//                     Ok(data) => {
//                         if let Some(streamer_message) = data.data {
//                             sender
//                                 .send(streamer_message)
//                                 .await
//                                 .expect("Error sending message");
//                         }
//                         start_block_height += 1;
//                         let block_height = current_block_height;
//                         let fast_near_client_clone = fast_near_client.clone();
//                         let task = tokio::spawn(async move {
//                             fast_near_client_clone.get_object(block_height).await
//                         });
//                         pending.insert(block_height, task);
//                         current_block_height += 1;
//                     }
//                     Err(err) => match err {
//                         FastNearError::BlockHeightTooHigh(_)
//                         | FastNearError::BlockDoesNotExist(_) => {
//                             let block_height = start_block_height;
//                             let fast_near_client_clone = fast_near_client.clone();
//                             let retry_task = tokio::spawn(async move {
//                                 tokio::time::sleep(std::time::Duration::from_millis(500)).await;
//                                 fast_near_client_clone.get_object(block_height).await
//                             });
//                             pending.insert(start_block_height, retry_task);
//                         }
//                         _ => {
//                             tracing::error!("Error to get block {} - {}", start_block_height, err);
//                             return;
//                         }
//                     },
//                 },
//                 Err(err) => {
//                     tracing::error!("Error to get block {} - {}", start_block_height, err);
//                     return;
//                 }
//             }
//         }
//     }
// }

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const LOG_TARGET: &str = "neardata-fetcher";
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn start_fetcher(
    // client: Option<FastNearClient>,
    config: FastNearConfig,
    blocks_sink: mpsc::Sender<near_indexer_primitives::StreamerMessage>,
    // is_running: std::sync::Arc<AtomicBool>,
) {
    let is_running = std::sync::Arc::new(AtomicBool::new(true));
    let client = FastNearClient::new(&config);;
    let max_num_threads = config.blocks_preload_pool_size as u64;
    let next_sink_block = std::sync::Arc::new(AtomicU64::new(config.start_block_height));
    while is_running.load(Ordering::SeqCst) {
        let start_block_height = next_sink_block.load(Ordering::SeqCst);
        let next_fetch_block = std::sync::Arc::new(AtomicU64::new(start_block_height));
        let last_block_height = client.get_last_block()
            .await
            .expect("Last block doesn't exist")
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
                let is_running = is_running.clone();
                tokio::spawn(async move {
                    while is_running.load(Ordering::SeqCst) {
                        let block_height = next_fetch_block.fetch_add(1, Ordering::SeqCst);
                        if is_backfill && block_height > last_block_height {
                            break;
                        }
                        tracing::debug!(target: LOG_TARGET, "#{}: Fetching block: {}", thread_index, block_height);
                        let block =
                            client.get_block_by_height(block_height, DEFAULT_TIMEOUT).await;
                        while is_running.load(Ordering::SeqCst) {
                            let expected_block_height = next_sink_block.load(Ordering::SeqCst);
                            if expected_block_height < block_height {
                                tokio::time::sleep(Duration::from_millis(
                                    block_height - expected_block_height,
                                ))
                                    .await;
                            } else {
                                tracing::debug!(target: LOG_TARGET, "#{}: Sending block: {}", thread_index, block_height);
                                break;
                            }
                        }
                        if !is_running.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Some(block) = block {
                            blocks_sink.send(block).await.expect("Failed to send block");
                        } else {
                            tracing::debug!(target: LOG_TARGET, "#{}: Skipped block: {}", thread_index, block_height);
                        }
                        next_sink_block.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.expect("Failed to join fetching thread");
        }
    }
}