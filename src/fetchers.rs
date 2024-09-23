#[derive(Clone, Debug)]
pub struct FastNearClient {
    client: reqwest::Client,
    endpoint: String,
}

impl FastNearClient {
    pub fn new(config: &crate::types::FastNearConfig) -> Self {
        Self {
            endpoint: config.endpoint.clone(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_object(
        &self,
        url: String,
    ) -> Result<Option<near_indexer_primitives::StreamerMessage>, crate::types::FastNearError> {
        let response = self.client.get(&url).send().await?;
        match response.status().as_u16() {
            200 => {
                let json: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| crate::types::FastNearError::UnknownError(err.to_string()))?;
                if json.is_null() {
                    Ok(None)
                } else {
                    Ok(serde_json::from_value(json).map_err(|err| {
                            crate::types::FastNearError::UnknownError(err.to_string())
                        })?)
                }
            }
            404 => {
                let error_response: crate::types::ErrorResponse =
                    serde_json::from_value(response.json().await.map_err(|err| {
                        crate::types::FastNearError::UnknownError(err.to_string())
                    })?)
                    .map_err(|err| crate::types::FastNearError::UnknownError(err.to_string()))?;
                Err(error_response.into())
            }
            _ => Err(crate::types::FastNearError::UnknownError(format!(
                "Unexpected status code: {}",
                response.status()
            ))),
        }
    }

    pub async fn fetch_block_until_success(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Option<near_indexer_primitives::StreamerMessage> {
        loop {
            match fetch_block(client, url, timeout).await {
                Ok(block) => return block,
                Err(FetchError::ReqwestError(err)) => {
                    tracing::warn!(target: LOG_TARGET, "Failed to fetch block: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn get_last_block(
        &self,
    ) -> Result<near_indexer_primitives::StreamerMessage, crate::types::FastNearError> {
        let url = format!("{}/v0/last_block/final", self.endpoint);
        let fast_near_data = self.get_object(url).await?;
        if let Some(data) = fast_near_data {
            Ok(data)
        } else {
            Err(crate::types::FastNearError::BlockDoesNotExist("Block final does not exist".to_string()))
        }
    }

    pub async fn get_streamer_message(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> Result<near_indexer_primitives::StreamerMessage, crate::types::FastNearError> {
        let url = format!("{}/v0/block/{}", self.endpoint, block_height);
        let fast_near_data = self.get_object(url).await?;
        if let Some(data) = fast_near_data {
            Ok(data)
        } else {
            Err(crate::types::FastNearError::BlockDoesNotExist(format!(
                "Block {} does not exist",
                block_height,
            )))
        }
    }

    pub async fn get_block_by_height(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> Result<near_indexer_primitives::StreamerMessage, crate::types::FastNearError> {
        let url = format!("{}/v0/block/{}", self.endpoint, block_height);
        let fast_near_data = self.get_object(url).await?;
        if let Some(data) = fast_near_data {
            Ok(data)
        } else {
            Err(crate::types::FastNearError::BlockDoesNotExist(format!(
                "Block {} does not exist",
                block_height,
            )))
        }
    }

    pub async fn get_shard(
        &self,
        block_height: crate::types::BlockHeight,
        shard_id: u64,
    ) -> Result<near_indexer_primitives::IndexerShard, crate::types::FastNearError> {
        let url = format!("{}/v0/block/{}", self.endpoint, block_height);
        let fast_near_data = self.get_object(url).await?;
        if let Some(data) = fast_near_data {
            Ok(data
                .shards
                .iter()
                .filter_map(|shard| {
                    if shard.shard_id == shard_id {
                        Some(shard.clone())
                    } else {
                        None
                    }
                })
                .next()
                .ok_or_else(|| {
                    crate::types::FastNearError::BlockDoesNotExist(format!(
                        "Block {} and shard {} does not exist",
                        block_height, shard_id
                    ))
                })?)
        } else {
            Err(crate::types::FastNearError::BlockDoesNotExist(format!(
                "Block {} does not exist",
                block_height,
            )))
        }
    }
}
