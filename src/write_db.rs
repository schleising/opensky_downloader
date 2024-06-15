use std::error::Error;
use std::time::{Instant, Duration};

use mongodb::{Client, Database, Collection};
use mongodb::IndexModel;

use bson::doc;

use crate::models::Aircraft;

pub async fn write_to_db(aircraft_vec: Vec<Aircraft>) -> Result<(), Box<dyn Error>> {
    // Create a MongoDB client
    let client: Client = Client::with_uri_str("mongodb://macmini2:27017").await?;

    // Get the database
    let db: Database = client.database("web_database");

    // Get the collection
    let collection: Collection<Aircraft> = db.collection("aircraft_collection");

    // Drop the collection if it already exists
    println!("Dropping the collection...");
    collection.drop(None).await?;

    // Start a timer
    let start: Instant = Instant::now();
    // Insert the aircraft into the collection
    println!("Inserting {} documents into the database...", aircraft_vec.len());
    collection.insert_many(aircraft_vec, None).await?;
    // Stop the timer
    let duration: Duration = start.elapsed();
    println!("Inserted documents in {:?}", duration);

    // Create an index on the registration field
    println!("Creating an index on the registration field...");
    let index: IndexModel = IndexModel::builder().keys(doc! { "registration": 1 }).build();
    collection.create_index(index, None).await?;

    // Return Ok
    return Ok(());
}
