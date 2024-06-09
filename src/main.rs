mod downloader;
mod models;
mod write_db;

use std::process::exit;

use downloader::download;
use models::Aircraft;
use write_db::write_to_db;

enum ExitCodes {
    DownloadError = 1,
    WriteError = 2,
}

fn main() {
    // URL to download the file from
    let url: &str = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv";

    // Initialise the aircraft vector
    let aircraft_vec: Vec<Aircraft>;

    // Download the file
    match download(url) {
        Ok(result) => {
            // Print a success message
            println!("Downloaded successfully");

            // Assign the result to the aircraft vector
            aircraft_vec = result;
        },
        Err(e) => {
            // Print an error message and exit
            eprintln!("Error: {}", e);
            exit(ExitCodes::DownloadError as i32);
        },
    }

    match write_to_db(aircraft_vec) {
        // Print a success message
        Ok(_) => println!("Wrote to database successfully"),
        Err(e) => {
            // Print an error message and exit
            eprintln!("Error: {}", e);
            exit(ExitCodes::WriteError as i32);
        },
    }
}
