use std::error::Error;
use std::time::{Instant, Duration};

use crate::models::Aircraft;

pub async fn download(url: &str) -> Result<Vec<Aircraft>, Box<dyn Error>> {
    // Download the file
    println!("Downloading file from {}...", url);
    let csv_data: String = download_file(url).await?;

    // Parse the file
    println!("Downloaded {} bytes, parsing data...", csv_data.len());
    // Start a timer
    let start: Instant = Instant::now();
    let aircraft_vec: Vec<Aircraft> = parse_file(&csv_data)?;
    // Stop the timer
    let duration: Duration = start.elapsed();
    println!("Parsed {} records in {:?}", aircraft_vec.len(), duration);

    Ok(aircraft_vec)
}

async fn download_file(url: &str) -> Result<String, Box<dyn Error>> {
    // Create a reqwest client
    let client: reqwest::Client = reqwest::ClientBuilder::new().build()?;

    // Send a GET request to the URL
    let response: reqwest::Response = client.get(url).send().await?;

    // Check if the request was successful
    if !response.status().is_success() {
        return Err(format!("Failed to download file: {}", response.status()).into());
    }

    // Start a timer
    let start: Instant = Instant::now();
    // Read the body of the response
    let body: String = response.text().await?;
    // Stop the timer
    let duration: Duration = start.elapsed();
    println!("Got body in {:?}", duration);

    // Return the body
    Ok(body)
}

fn parse_file(csv_data: &str) -> Result<Vec<Aircraft>, Box<dyn Error>> {
    // Create a CSV reader
    let mut reader: csv::Reader<&[u8]> = csv::Reader::from_reader(csv_data.as_bytes());

    // Create a vector to store the aircraft
    let mut aircraft_vec: Vec<Aircraft> = Vec::new();

    // Iterate over the records deserialising them into Aircraft structs
    for result in reader.deserialize() {
        // Make sure the record is of type Aircraft and is mutable
        let mut aircraft: Aircraft = result?;

        // Convert the icao24 to lowercase
        aircraft.icao24 = aircraft.icao24.to_lowercase();

        // If the icao24 field is not empty push the aircraft into the vector
        if !aircraft.icao24.is_empty() {
            // Push the aircraft into the vector
            aircraft_vec.push(aircraft);
        }
    }

    // Return the vector
    Ok(aircraft_vec)
}
