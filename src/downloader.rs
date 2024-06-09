use std::error::Error;

use crate::models::Aircraft;

pub fn download(url: &str) -> Result<Vec<Aircraft>, Box<dyn Error>> {
    println!("Downloading file from {}", url);

    // Download the file
    let csv_data: String = download_file(url)?;

    // Parse the file
    let aircraft_vec: Vec<Aircraft> = parse_file(&csv_data)?;

    // Print the first 10 lines
    for aircraft in aircraft_vec.iter().take(5) {
        println!("{}", aircraft);
    }

    Ok(aircraft_vec)
}

fn download_file(url: &str) -> Result<String, Box<dyn Error>> {
    // Create a reqwest client
    let client: reqwest::blocking::Client = reqwest::blocking::ClientBuilder::new().build()?;

    // Send a GET request to the URL
    let mut response: reqwest::blocking::Response = client.get(url).send()?;

    // Create a buffer to store the response body
    let mut buffer: Vec<u8> = Vec::new();

    // Copy the response body into the buffer
    response.copy_to(&mut buffer)?;

    // Convert the buffer into a string
    let body: String = String::from_utf8(buffer)?;

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
