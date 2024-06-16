use std::error::Error;
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

fn response_to_async_read(resp: reqwest::Response) -> impl tokio::io::AsyncRead {
    use futures::stream::TryStreamExt;

    let stream = resp.bytes_stream().map_err(std::io::Error::other);
    tokio_util::io::StreamReader::new(stream)
}

pub async fn download(url: &str) -> Result<(), Box<dyn Error>> {
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
    if !response.status().is_success() {
        return Err(format!("Failed to download file: {}", response.status()).into());
    }

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

    // Create a vector to store 1000 records at a time
    let mut records_vec: Vec<Aircraft> = Vec::new();

    println!("{}", "Downloading records...".blue().bold());

    // Start a timer
    let start: Instant = Instant::now();

    while let Some(record) = records.next().await {
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

        // If the vector has 1000 records, insert them into the collection
        if records_vec.len() == MAX_RECORDS {
            let collection = arc_collection.clone();
            let records_to_insert = records_vec.clone();

            join_handles.push(tokio::spawn(async move {
                // Insert the aircraft into the collection
                match collection.insert_many(records_to_insert, None).await {
                    Ok(_) => {}
                    Err(e) => {
                        let error: String = format!("Failed to insert aircraft: {}", e);
                        eprintln!("{}", error.red().bold());
                    }
                }
            }));

            // Clear the vector
            records_vec.clear();
        }
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Downloaded records in {:?}", duration);
    println!("{}", text.green().bold());

    println!("{}", "Inserting records...".blue().bold());


    // Start a timer
    let start: Instant = Instant::now();

    // Insert the remaining records into the collection
    if !records_vec.is_empty() {
        // Clone the collection
        let collection: Arc<Collection<Aircraft>> = arc_collection.clone();

        // Insert the aircraft into the collection
        match collection.insert_many(records_vec, None).await {
            Ok(_) => {}
            Err(e) => {
                let error: String = format!("Failed to insert aircraft: {}", e);
                eprintln!("{}", error.red().bold());
            }
        }
    }

    // Wait for all the join handles to finish
    for join_handle in join_handles {
        join_handle.await?;
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Inserted records in {:?}", duration);
    println!("{}", text.green().bold());

    Ok(())
}
