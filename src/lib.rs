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
    (tokio::spawn(start(config, sender)), receiver)
}

/// Spawns a task that sends objects in strict sequential order to the `mpsc` channel
/// by fetching in concurrent batches and maintaining ordering.
async fn start(
    config: FastNearConfig,
    sender: mpsc::Sender<near_indexer_primitives::StreamerMessage>,
) {
    let fast_near_client = FastNearClient::new(&config);
    let mut current_block_height = config.start_block_height;
    let mut start_block_height = config.start_block_height;
    let mut pending: std::collections::HashMap<
        types::BlockHeight,
        tokio::task::JoinHandle<Result<types::FastNearData, types::FastNearError>>,
    > = std::collections::HashMap::new();
    loop {
        // Fill the pending queue with new fetch tasks until the blocks_preload_pool_size
        while pending.len() < config.blocks_preload_pool_size {
            let block_height = current_block_height;
            let fast_near_client_clone = fast_near_client.clone();
            let task =
                tokio::spawn(async move { fast_near_client_clone.get_object(block_height).await });
            pending.insert(block_height, task);
            current_block_height += 1;
        }

        // Process objects in strict order by block_height
        if let Some(task) = pending.remove(&start_block_height) {
            match task.await {
                Ok(result) => match result {
                    Ok(data) => {
                        if let Some(streamer_message) = data.data {
                            sender
                                .send(streamer_message)
                                .await
                                .expect("Error sending message");
                        }
                        start_block_height += 1;
                        let block_height = current_block_height;
                        let fast_near_client_clone = fast_near_client.clone();
                        let task = tokio::spawn(async move {
                            fast_near_client_clone.get_object(block_height).await
                        });
                        pending.insert(block_height, task);
                        current_block_height += 1;
                    }
                    Err(err) => match err {
                        FastNearError::BlockHeightTooHigh(_)
                        | FastNearError::BlockDoesNotExist(_) => {
                            let block_height = start_block_height;
                            let fast_near_client_clone = fast_near_client.clone();
                            let retry_task = tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                fast_near_client_clone.get_object(block_height).await
                            });
                            pending.insert(start_block_height, retry_task);
                        }
                        _ => {
                            tracing::error!("Error to get block {} - {}", start_block_height, err);
                            return;
                        }
                    },
                },
                Err(err) => {
                    tracing::error!("Error to get block {} - {}", start_block_height, err);
                    return;
                }
            }
        }
    }
}
