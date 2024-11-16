use reqwest::{Client, ClientBuilder, Response};

use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio::task;
use tokio_util::io::StreamReader;

use futures::stream::{StreamExt, TryStreamExt};

use serde::de::DeserializeOwned;

use csv_async::{self, DeserializeRecordsStreamPos};

// Errors that can occur
pub enum DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    ReqwestError(reqwest::Error),
    CsvError(csv_async::Error),
    SendError(mpsc::error::SendError<RecordInfo<D>>),
    ZeroLengthError,
    ChannelError,
}

impl<D> From<reqwest::Error> for DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn from(error: reqwest::Error) -> Self {
        DownloadError::ReqwestError(error)
    }
}

impl<D> From<csv_async::Error> for DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn from(error: csv_async::Error) -> Self {
        DownloadError::CsvError(error)
    }
}

impl<D> From<mpsc::error::SendError<RecordInfo<D>>> for DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn from(error: mpsc::error::SendError<RecordInfo<D>>) -> Self {
        DownloadError::SendError(error)
    }
}

impl<D> From<DownloadError<D>> for std::io::Error
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn from(error: DownloadError<D>) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, error)
    }
}

impl<D> std::fmt::Display for DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DownloadError::ReqwestError(e) => write!(f, "Reqwest error: {}", e),
            DownloadError::CsvError(e) => write!(f, "CSV error: {}", e),
            DownloadError::SendError(e) => write!(f, "Send error: {}", e),
            DownloadError::ZeroLengthError => write!(f, "The content length is zero"),
            DownloadError::ChannelError => write!(f, "Channel error"),
        }
    }
}

impl<D> std::fmt::Debug for DownloadError<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DownloadError::ReqwestError(e) => write!(f, "Reqwest error: {}", e),
            DownloadError::CsvError(e) => write!(f, "CSV error: {}", e),
            DownloadError::SendError(e) => write!(f, "Send error: {}", e),
            DownloadError::ZeroLengthError => write!(f, "The content length is zero"),
            DownloadError::ChannelError => write!(f, "Channel error"),
        }
    }
}

impl<D> std::error::Error for DownloadError<D> where D: DeserializeOwned + Send + Sync + 'static {}

pub struct DownloadInfo<D> {
    pub content_length: u64,
    pub rx_channel: mpsc::UnboundedReceiver<RecordInfo<D>>,
    tx_channel: Option<mpsc::UnboundedSender<RecordInfo<D>>>,
}

pub struct RecordInfo<D> {
    pub record: D,
    pub position: u64,
}

impl<D> DownloadInfo<D>
where
    D: DeserializeOwned + Send + Sync + 'static,
{
    pub fn new() -> Self {
        // Create a tokio channel to send records to
        let (tx, rx) = mpsc::unbounded_channel::<RecordInfo<D>>();

        DownloadInfo {
            content_length: 0,
            rx_channel: rx,
            tx_channel: Some(tx),
        }
    }

    pub async fn download(
        &mut self,
        url: &str,
    ) -> Result<task::JoinHandle<Result<(), DownloadError<D>>>, DownloadError<D>> {
        // Create a reqwest client
        let http_client: Client = ClientBuilder::new().build()?;

        // Send a GET request to the URL
        let response: Response = http_client.get(url).send().await?.error_for_status()?;

        // Get the content length
        self.content_length = response.content_length().ok_or(DownloadError::ZeroLengthError)?;

        // Clone the tx_channel, or return an error
        let tx_channel = self.tx_channel.clone().ok_or(DownloadError::ChannelError)?;

        // Set the tx_channel in the struct to None to drop it, the clone is used in the task and will be dropped when the task is done
        self.tx_channel = None;

        // Spawn a tokio task to iterate over the records
        let join_handle = tokio::spawn(async move {
            // Get the response as a stream of bytes
            let bytes_stream = response
                .bytes_stream()
                .map_err(DownloadError::<D>::ReqwestError);

            // Convert the stream of bytes to an AsyncRead
            let stream_reader = StreamReader::new(bytes_stream);

            // Create a CSV reader
            // let mut csv_reader = csv_async::AsyncDeserializer::from_reader(stream_reader);
            let mut csv_reader = csv_async::AsyncReaderBuilder::new()
                .quote(b'\'')
                .create_deserializer(stream_reader);

            // Create a deserializer
            let mut records = csv_reader.deserialize_with_pos::<D>();

            // Iterate over the records
            iterate_records(&mut records, tx_channel).await?;

            // Return Ok
            Ok(())
        });

        // Return the content length
        return Ok(join_handle);
    }
}

async fn iterate_records<'r, R, D>(
    records: &mut DeserializeRecordsStreamPos<'r, R, D>,
    tx_channel: mpsc::UnboundedSender<RecordInfo<D>>,
) -> Result<(), DownloadError<D>>
where
    R: AsyncRead + Send + Unpin,
    D: DeserializeOwned + Send + Sync + 'static,
{
    // Iterate over the records
    while let Some((record, pos)) = records.next().await {
        // Get the record
        let record = record?;

        // Send the record over a channel to be processed
        let record_info = RecordInfo {
            record,
            position: pos.byte(),
        };

        // Send the record over the channel
        tx_channel.send(record_info)?;
    }

    // Return Ok
    Ok(())
}
