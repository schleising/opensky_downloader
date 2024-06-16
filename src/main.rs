mod downloader;
mod models;

use std::process::exit;
use std::time::{Duration, Instant};

use colored::Colorize;

use downloader::download;
enum ExitCodes {
    DownloadError = 1,
}

#[tokio::main]
async fn main() {
    // Start a timer
    let start: Instant = Instant::now();

    // URL to download the file from
    let url: &str = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv";

    // Download the file
    match download(url).await {
        Ok(_) => {}
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
        }
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Program ran in {:?}", duration);
    println!("{}", text.blue().bold());
}
