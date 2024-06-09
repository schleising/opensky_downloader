use std::error::Error;

use mongodb::sync::Client;
use mongodb::IndexModel;

use bson::doc;

use crate::models::Aircraft;

pub fn write_to_db(aircraft_vec: Vec<Aircraft>) -> Result<(), Box<dyn Error>> {
    // Create a MongoDB client
    let client: Client = Client::with_uri_str("mongodb://macmini2:27017")?;

    // Get the database
    let db: mongodb::sync::Database = client.database("web_database");

    // Get the collection
    let collection: mongodb::sync::Collection<Aircraft> = db.collection("aircraft_collection");

    // Drop the collection if it already exists
    println!("Dropping the collection...");
    collection.drop(None)?;

    // Insert the aircraft into the collection
    println!("Inserting {} documents into the database...", aircraft_vec.len());
    collection.insert_many(aircraft_vec, None)?;

    // Create an index on the registration field
    println!("Creating an index on the registration field...");
    let index = IndexModel::builder().keys(doc! { "registration": 1 }).build();
    collection.create_index(index, None)?;

    // Return Ok
    return Ok(());
}
