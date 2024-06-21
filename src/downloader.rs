use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util;

use mongodb::IndexModel;
use mongodb::{Client, Collection, Database};

use bson::doc;

use colored::Colorize;

use crate::models::Aircraft;

// Constants
const MONGO_URI: &str = "mongodb://macmini2:27017";
const DATABASE_NAME: &str = "web_database";
const COLLECTION_NAME: &str = "aircraft_collection";
const MAX_RECORDS: usize = 1000;

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

fn response_to_async_read(resp: reqwest::Response) -> impl tokio::io::AsyncRead {
    use futures::stream::TryStreamExt;

    let stream = resp.bytes_stream().map_err(std::io::Error::other);
    tokio_util::io::StreamReader::new(stream)
}

pub async fn download(url: &str) -> Result<(), DownloadError> {
    let text = format!("Downloading file from {}...", url);
    println!("{}", text.blue().bold());

    // Create a reqwest client
    let http_client: reqwest::Client = reqwest::ClientBuilder::new().build()?;

    // Create a mongodb client
    let mongo_client: Client = Client::with_uri_str(MONGO_URI).await?;

    // Get the database
    let db: Database = mongo_client.database(DATABASE_NAME);

    // Get the collection
    let collection: Collection<Aircraft> = db.collection(COLLECTION_NAME);

    // Send a GET request to the URL
    let response: reqwest::Response = http_client.get(url).send().await?;

    // Check if the request was successful
    response.error_for_status_ref()?;

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
    let mut records = csv_reader.deserialize::<Aircraft>();

    // Create an Arc to share the collection between tasks
    let arc_collection: Arc<Collection<Aircraft>> = Arc::new(collection);

    // Create a vector to store the join handles
    let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

    
    println!("{}", "Downloading records...".blue().bold());
    
    // Start a timer
    let start: Instant = Instant::now();
    
    let mut finished = false;
    
    while !finished {
        // Create a vector to store 1000 records at a time
        let mut records_vec: Vec<Aircraft> = Vec::new();

        while records_vec.len () < MAX_RECORDS {
            match records.next().await {
                Some(record) => {
                    // Unwrap the record
                    let mut record: Aircraft = record?;

                    // Check if the icao24 field is empty, skip the record if it is
                    if record.icao24.is_empty() {
                        continue;
                    }

                    // Make sure the icao24 field is uppercase
                    record.icao24 = record.icao24.to_uppercase();

                    // Push the record into the vector
                    records_vec.push(record);
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }

        if records_vec.len() > 0 {
            // Clone the Arc to share the collection between tasks
            let collection: Arc<Collection<Aircraft>> = arc_collection.clone();

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
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Downloaded records in {:?}", duration);
    println!("{}", text.green().bold());

    println!("{}", "Waiting for tasks to finish...".blue().bold());

    // Start a timer
    let start: Instant = Instant::now();

    // Wait for all the join handles to finish
    for join_handle in join_handles {
        join_handle.await?;
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Tasks finished in {:?}", duration);
    println!("{}", text.green().bold());

    Ok(())
}
