use futures::StreamExt;

pub async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    stats: std::sync::Arc<tokio::sync::RwLock<Stats>>,
) -> anyhow::Result<()> {
    stats
        .write()
        .await
        .block_heights_processing
        .insert(streamer_message.block.header.height);

    let mut stats_lock = stats.write().await;
    stats_lock
        .block_heights_processing
        .remove(&streamer_message.block.header.height);
    stats_lock.blocks_processed_count += 1;
    stats_lock.last_processed_block_height = streamer_message.block.header.height;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(Stats::default()));
    tokio::spawn(state_logger(std::sync::Arc::clone(&stats)));

    let config = fastnear_data_framework::FastNearConfigBuilder::default()
        .mainnet()
        .start_block_height(127792400)
        .build()
        .expect("Failed to build FastNearConfig");

    let (sender, stream) = fastnear_data_framework::streamer(&config);

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(streamer_message, std::sync::Arc::clone(&stats))
        })
        .buffer_unordered(2);

    while let Some(_handle_message) = handlers.next().await {
        if let Err(err) = _handle_message {
            tracing::error!("Error handling streamer message: {:?}", err);
        }
    }

    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub block_heights_processing: std::collections::BTreeSet<u64>,
    pub blocks_processed_count: u64,
    pub last_processed_block_height: u64,
}

pub async fn state_logger(stats: std::sync::Arc<tokio::sync::RwLock<Stats>>) {
    let interval_secs = 10;
    let mut prev_blocks_processed_count: u64 = 0;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        let stats_lock = stats.read().await;

        let block_processing_speed: f64 = ((stats_lock.blocks_processed_count
            - prev_blocks_processed_count) as f64)
            / (interval_secs as f64);

        println!(
            "# {} | Blocks processing: {}| Blocks done: {}. Bps {:.2} b/s",
            stats_lock.last_processed_block_height,
            stats_lock.block_heights_processing.len(),
            stats_lock.blocks_processed_count,
            block_processing_speed,
        );
        prev_blocks_processed_count = stats_lock.blocks_processed_count;
    }
}
