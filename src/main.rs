mod downloader;
mod models;
mod write_db;

use std::process::exit;
use std::time::{Instant, Duration};

use colored::Colorize;

use downloader::download;
use models::Aircraft;
use write_db::write_to_db;

enum ExitCodes {
    DownloadError = 1,
    WriteError = 2,
}

#[tokio::main]
async fn main() {
    // Start a timer
    let start: Instant = Instant::now();

    // URL to download the file from
    let url: &str = "https://www.schleising.net/aircraftDatabase.csv";

    // Initialise the aircraft vector
    let aircraft_vec: Vec<Aircraft>;

    // Download the file
    match download(url).await {
        Ok(result) => {
            // Print a success message
            println!("{}", "Downloaded successfully".green().bold());

            // Assign the result to the aircraft vector
            aircraft_vec = result;
        },
        Err(e) => {
            // Print an error message and exit
            let error: String = format!("Error: {}", e);
            eprintln!("{}", error.red().bold());
            
            // Stop the timer
            let duration: Duration = start.elapsed();
            let text: String = format!("Program ran in {:?}", duration);
            println!("{}", text.blue().bold());

            // Exit with the DownloadError exit code
            exit(ExitCodes::DownloadError as i32);
        },
    }

    match write_to_db(aircraft_vec).await {
        // Print a success message
        Ok(_) => println!("{}", "Wrote to database successfully".green().bold()),
        Err(e) => {
            // Print an error message and exit
            let error: String = format!("Error: {}", e);
            eprintln!("{}", error.red().bold());

            // Stop the timer
            let duration: Duration = start.elapsed();
            let text: String = format!("Program ran in {:?}", duration);
            println!("{}", text.blue().bold());

            // Exit with the WriteError exit code
            exit(ExitCodes::WriteError as i32);
        },
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Program ran in {:?}", duration);
    println!("{}", text.blue().bold());
}
