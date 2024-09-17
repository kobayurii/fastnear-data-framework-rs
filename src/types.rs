/// Type alias represents the block height
pub type BlockHeight = u64;

pub struct FastNearData {
    pub block_height: BlockHeight,
    pub data: Option<near_indexer_primitives::StreamerMessage>,
}

/// Configuration struct for Fast NEAR Data Framework
/// NB! Consider using [`FastNearConfigBuilder`]
/// Building the `FastNearConfig` example:
/// ```
/// use fastnear_data_framework::FastNearConfigBuilder;
///
/// # async fn main() {
///    let config = FastNearConfigBuilder::default()
///        .testnet()
///        .start_block_height(82422587)
///        .build()
///        .expect("Failed to build FastNearConfig");
/// # }
/// ```
#[derive(Default, Builder, Debug)]
#[builder(pattern = "owned")]
pub struct FastNearConfig {
    /// Fastnear data endpoint
    #[builder(setter(into))]
    pub(crate) endpoint: String,
    /// Defines the block height to start indexing from
    pub(crate) start_block_height: u64,
    #[builder(default = "100")]
    pub(crate) blocks_preload_pool_size: usize,
}

impl FastNearConfigBuilder {
    /// Shortcut to set up [FastNearConfigBuilder] for mainnet
    /// ```
    /// use fastnear_data_framework::FastNearConfigBuilder;
    ///
    /// # async fn main() {
    ///    let config = FastNearConfigBuilder::default()
    ///        .mainnet()
    ///        .start_block_height(65231161)
    ///        .build()
    ///        .expect("Failed to build FastNearConfig");
    /// # }
    /// ```
    pub fn mainnet(mut self) -> Self {
        self.endpoint = Some("https://mainnet.neardata.xyz".to_string());
        self
    }

    /// Shortcut to set up [FastNearConfigBuilder] for testnet
    /// ```
    /// use fastnear_data_framework::FastNearConfigBuilder;
    ///
    /// # async fn main() {
    ///    let config = FastNearConfigBuilder::default()
    ///        .testnet()
    ///        .start_block_height(82422587)
    ///        .build()
    ///        .expect("Failed to build FastNearConfig");
    /// # }
    /// ```
    pub fn testnet(mut self) -> Self {
        self.endpoint = Some("https://testnet.neardata.xyz".to_string());
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FastNearError {
    #[error("Block height too high: {0}")]
    BlockHeightTooHigh(String),
    #[error("Block height too low: {0}")]
    BlockHeightTooLow(String),
    #[error("Block does not exist: {0}")]
    BlockDoesNotExist(String),
    #[error("Request error: {source}")]
    RequestError {
        #[from]
        source: reqwest::Error,
    },
    #[error("An unknown error occurred: {0}")]
    UnknownError(String),
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ErrorResponse {
    error: String,
    #[serde(rename = "type")]
    error_type: String,
}

impl From<ErrorResponse> for FastNearError {
    fn from(response: ErrorResponse) -> Self {
        match response.error_type.as_str() {
            "BLOCK_DOES_NOT_EXIST" => FastNearError::BlockDoesNotExist(response.error),
            "BLOCK_HEIGHT_TOO_HIGH" => FastNearError::BlockHeightTooHigh(response.error),
            "BLOCK_HEIGHT_TOO_LOW" => FastNearError::BlockHeightTooLow(response.error),
            _ => FastNearError::UnknownError(response.error),
        }
    }
}
