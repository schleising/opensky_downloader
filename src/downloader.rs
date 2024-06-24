use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use mongodb::IndexModel;
use mongodb::{Client, Collection, Database};

use bson::doc;

use colored::Colorize;

use indicatif::{style, ProgressBar};

// Constants
const MONGO_URI: &str = "mongodb://macmini2:27017";
const DATABASE_NAME: &str = "web_database";
const COLLECTION_NAME: &str = "aircraft_collection";
const MAX_RECORDS: usize = 1000;

pub trait FilterMap {
    fn filter(&self) -> bool;
    fn map(&mut self);
}

// Errors that can occur
#[derive(Debug)]
pub enum DownloadError {
    ReqwestError(reqwest::Error),
    MongoError(mongodb::error::Error),
    CsvError(csv_async::Error),
    JoinError(tokio::task::JoinError),
}

impl From<reqwest::Error> for DownloadError {
    fn from(error: reqwest::Error) -> Self {
        DownloadError::ReqwestError(error)
    }
}

impl From<mongodb::error::Error> for DownloadError {
    fn from(error: mongodb::error::Error) -> Self {
        DownloadError::MongoError(error)
    }
}

impl From<csv_async::Error> for DownloadError {
    fn from(error: csv_async::Error) -> Self {
        DownloadError::CsvError(error)
    }
}

impl From<tokio::task::JoinError> for DownloadError {
    fn from(error: tokio::task::JoinError) -> Self {
        DownloadError::JoinError(error)
    }
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DownloadError::ReqwestError(e) => write!(f, "Reqwest error: {}", e),
            DownloadError::MongoError(e) => write!(f, "Mongo error: {}", e),
            DownloadError::CsvError(e) => write!(f, "CSV error: {}", e),
            DownloadError::JoinError(e) => write!(f, "Join error: {}", e),
        }
    }
}

impl std::error::Error for DownloadError {}

async fn receive_records<'r, R, D>(
    records: &mut csv_async::DeserializeRecordsStreamPos<'r, R, D>,
    arc_collection: Arc<Collection<D>>,
    join_handles: &mut Vec<JoinHandle<()>>,
    progress_bar: &Option<ProgressBar>,
) -> Result<bool, DownloadError>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    D: DeserializeOwned + Serialize + Send + Sync + FilterMap + 'static,
{
    // True if the records are finished
    let mut finished = false;

    // Create a vector to store 1000 records at a time
    let mut records_vec: Vec<D> = Vec::new();

    while records_vec.len() < MAX_RECORDS {
        match records.next().await {
            Some((record, pos)) => {
                // Unwrap the record
                let mut record: D = record?;

                // Filter out unwanted records
                if !record.filter() {
                    continue;
                }

                // Perform a mapping operation on the record
                record.map();

                // Push the record into the vector
                records_vec.push(record);

                // Print the progress
                if let Some(progress_bar) = progress_bar {
                    progress_bar.set_position(pos.byte());
                }
            }
            None => {
                finished = true;
                break;
            }
        }
    }

    if records_vec.len() > 0 {
        // Clone the Arc to share the collection between tasks
        let collection: Arc<Collection<D>> = arc_collection.clone();

        join_handles.push(tokio::spawn(async move {
            // Insert the aircraft into the collection
            match collection.insert_many(records_vec, None).await {
                Ok(_) => {}
                Err(e) => {
                    let error: String = format!("Failed to insert aircraft: {}", e);
                    eprintln!("{}", error.red().bold());
                }
            }
        }));
    }

    Ok(finished)
}

fn response_to_async_read(resp: reqwest::Response) -> impl tokio::io::AsyncRead {
    use futures::stream::TryStreamExt;

    let stream = resp.bytes_stream().map_err(std::io::Error::other);
    tokio_util::io::StreamReader::new(stream)
}

pub async fn download<T>(url: &str) -> Result<(), DownloadError>
where
    T: DeserializeOwned + Serialize + FilterMap + Send + Sync + 'static,
{
    let text = format!("Downloading file from {}...", url);
    println!("{}", text.blue().bold());

    // Create a reqwest client
    let http_client: reqwest::Client = reqwest::ClientBuilder::new().build()?;

    // Create a mongodb client
    let mongo_client: Client = Client::with_uri_str(MONGO_URI).await?;

    // Get the database
    let db: Database = mongo_client.database(DATABASE_NAME);

    // Get the collection
    let collection: Collection<T> = db.collection(COLLECTION_NAME);

    // Send a GET request to the URL
    let response: reqwest::Response = http_client.get(url).send().await?.error_for_status()?;

    // Get the content length
    let content_length: u64 = response.content_length().unwrap_or(0);

    // Drop the collection if it already exists
    println!(
        "{}",
        "Successful response, dropping the collection..."
            .green()
            .bold()
    );
    collection.drop(None).await?;

    // Create an index on the registration field
    println!(
        "{}",
        "Creating an index on the registration field..."
            .blue()
            .bold()
    );
    let index: IndexModel = IndexModel::builder()
        .keys(doc! { "registration": 1 })
        .build();
    collection.create_index(index, None).await?;

    // Convert the response to an async read
    let reader = response_to_async_read(response);

    // Create a CSV reader
    let mut csv_reader = csv_async::AsyncDeserializer::from_reader(reader);

    // Iterate over the records
    let mut records = csv_reader.deserialize_with_pos::<T>();

    // Create an Arc to share the collection between tasks
    let arc_collection: Arc<Collection<T>> = Arc::new(collection);

    // Create a vector to store the join handles
    let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

    // True if the records are finished
    let mut finished = false;

    // Create a progress bar
    let mut progress_bar: Option<ProgressBar>;

    if let Ok(progress_bar_style) = style::ProgressStyle::default_bar().template(
        "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})",
    ) {
        progress_bar = Some(ProgressBar::new(content_length).with_style(progress_bar_style).with_message("Downloading records"));
    } else {
        println!("{}", "Failed to create progress bar".red().bold());
        progress_bar = None;
    }

    while !finished {
        finished = receive_records(
            &mut records,
            arc_collection.clone(),
            &mut join_handles,
            &progress_bar,
        )
        .await?;
    }

    // Finish the progress bar
    if let Some(progress_bar) = progress_bar {
        progress_bar.finish();
    }

    // Create a progress bar for the join handles
    if let Ok(progress_bar_style) = style::ProgressStyle::default_bar().template(
        "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len})",
    ) {
        progress_bar = Some(ProgressBar::new(join_handles.len() as u64).with_style(progress_bar_style).with_message("Inserting records  "));
    } else {
        println!("{}", "Failed to create progress bar".red().bold());
        progress_bar = None;
    }

    // Wait for all the join handles to finish
    for join_handle in join_handles {
        join_handle.await?;
        if let Some(progress_bar) = &progress_bar {
            progress_bar.inc(1);
        }
    }

    // Finish the progress bar
    if let Some(progress_bar) = progress_bar {
        progress_bar.finish();
    }

    Ok(())
}
