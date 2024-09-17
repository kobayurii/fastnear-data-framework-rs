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
        block_height: crate::types::BlockHeight,
    ) -> Result<crate::types::FastNearData, crate::types::FastNearError> {
        let url = format!("{}/v0/block/{}", self.endpoint, block_height);
        let response = self.client.get(&url).send().await?;
        match response.status().as_u16() {
            200 => {
                let json: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| crate::types::FastNearError::UnknownError(err.to_string()))?;
                if json.is_null() {
                    Ok(crate::types::FastNearData {
                        block_height,
                        data: None,
                    })
                } else {
                    Ok(crate::types::FastNearData {
                        block_height,
                        data: serde_json::from_value(json).map_err(|err| {
                            crate::types::FastNearError::UnknownError(err.to_string())
                        })?,
                    })
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

    pub async fn get_streamer_message(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> Result<near_indexer_primitives::StreamerMessage, crate::types::FastNearError> {
        let fast_near_data = self.get_object(block_height).await?;
        if let Some(data) = fast_near_data.data {
            Ok(data)
        } else {
            Err(crate::types::FastNearError::BlockDoesNotExist(format!(
                "Block {} does not exist",
                block_height,
            )))
        }
    }

    pub async fn get_block(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> Result<near_indexer_primitives::views::BlockView, crate::types::FastNearError> {
        let fast_near_data = self.get_object(block_height).await?;
        if let Some(data) = fast_near_data.data {
            Ok(data.block)
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
        let fast_near_data: crate::types::FastNearData = self.get_object(block_height).await?;
        if let Some(data) = fast_near_data.data {
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
