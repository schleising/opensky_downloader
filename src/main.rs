mod db_writer;
mod models;
mod record_downloader;

use std::process::exit;
use std::time::{Duration, Instant};

use colored::Colorize;

use models::Aircraft;

use indicatif::{style, ProgressBar};

use record_downloader::DownloadInfo;

use db_writer::DatabaseWriter;

const MONGO_HOST: &str = "macmini2";
const DATABASE_NAME: &str = "web_database";
const COLLECTION_NAME: &str = "aircraft_collection";

enum ExitCodes {
    Success = 0,
    DownloadError = 1,
    DatabaseError = 2,
    JoinError = 3,
}

#[tokio::main]
async fn main() {
    // Start a timer
    let start: Instant = Instant::now();

    // Exit code
    let exit_code: ExitCodes;

    // Print that we are connecting to the database
    let text: String = format!("Connecting to MongoDB on {}", MONGO_HOST);
    println!("{}", text.blue().bold());

    // Create a new database writer
    match DatabaseWriter::<Aircraft>::new(MONGO_HOST, DATABASE_NAME, COLLECTION_NAME).await {
        Ok(mut db_writer) => {
            exit_code = download_and_store(&mut db_writer).await;
        }
        Err(error) => {
            let text = format!("Error: {}", error);
            eprintln!("{}", text.red().bold());
            exit_code = ExitCodes::DatabaseError;
        }
    }

    // Stop the timer
    let duration: Duration = start.elapsed();
    let text: String = format!("Program ran in {:?}", duration);
    println!("{}", text.blue().bold());

    exit(exit_code as i32);
}

async fn download_and_store(db_writer: &mut DatabaseWriter<Aircraft>) -> ExitCodes {
    // Exit code
    let mut exit_code: ExitCodes = ExitCodes::Success;

    // URL to download the file from
    let url: &str = "https://www.schleising.net/aircraftDatabase.csv";

    // Create a new DownloadInfo struct
    let mut download_info: DownloadInfo<Aircraft> = DownloadInfo::new();

    // Print that we are downloading the file
    let text: String = format!("Downloading file from {}", url);
    println!("{}", text.blue().bold());

    // Download the file
    match download_info.download(url).await {
        Ok(join_handle) => {
            // Print that we are dropping the collection
            let text: String = "URL found, dropping collection".to_string();
            println!("{}", text.blue().bold());

            // File found successfully, drop the collection
            match db_writer.drop_collection().await {
                Ok(_) => {
                    let text: String = "Collection dropped".to_string();
                    println!("{}", text.green().bold());
                }
                Err(error) => {
                    let text = format!("Error: {}", error);
                    eprintln!("{}", text.red().bold());
                    return ExitCodes::DatabaseError;
                }
            }

            // Print that we are creating an index
            let text: String = "Creating new index".to_string();
            println!("{}", text.blue().bold());

            // Create an index on the registration field
            match db_writer.create_index("registration").await {
                Ok(_) => {
                    let text: String = "Index created".to_string();
                    println!("{}", text.green().bold());
                }
                Err(error) => {
                    let text = format!("Error: {}", error);
                    eprintln!("{}", text.red().bold());
                    return ExitCodes::DatabaseError;
                }
            }

            // Handle the download
            handle_download(&mut download_info, db_writer).await;

            // Wait for the task to finish
            match join_handle.await {
                Ok(_) => {
                    let text: String = "Download complete".to_string();
                    println!("{}", text.green().bold());
                }
                Err(error) => {
                    let text = format!("Error: {}", error);
                    eprintln!("{}", text.red().bold());
                    exit_code = ExitCodes::JoinError;
                }
            }
        }
        Err(error) => {
            let text = format!("Error: {}", error);
            eprintln!("{}", text.red().bold());
            exit_code = ExitCodes::DownloadError;
        }
    }

    exit_code
}

async fn handle_download(
    download_info: &mut DownloadInfo<Aircraft>,
    db_writer: &mut DatabaseWriter<Aircraft>,
) {
    // Create a progress bar
    let progress_bar: Option<ProgressBar>;

    // Set up the progress bar
    if let Ok(progress_bar_style) = style::ProgressStyle::default_bar().template(
        "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})",
    ) {
        progress_bar = Some(ProgressBar::new(download_info.content_length).with_style(progress_bar_style).with_message("Downloading records"));
    } else {
        println!("{}", "Failed to create progress bar".red().bold());
        progress_bar = None;
    }

    // Download the file
    while let Some(mut record_info) = download_info.rx_channel.recv().await {
        // Print the progress
        if let Some(progress_bar) = &progress_bar {
            progress_bar.set_position(record_info.position);
        }

        // Increment the counter
        if record_info.record.icao24.is_empty() {
            continue;
        }

        // Convert the ICAO24 to uppercase
        record_info.record.icao24 = record_info.record.icao24.to_uppercase();

        // Insert the record into the database
        db_writer.add_record(record_info.record)
    }

    // Finish the progress bar
    if let Some(progress_bar) = &progress_bar {
        progress_bar.finish();
    }

    // Print that we are finishing writing the records
    let text: String = "Finishing writing records".to_string();
    println!("{}", text.blue().bold());

    // Finish writing the records
    match db_writer.finish().await {
        Ok(_) => {
            let text: String = "All records inserted".to_string();
            println!("{}", text.green().bold());
        }
        Err(error) => {
            let text = format!("Error: {}", error);
            eprintln!("{}", text.red().bold());
        }
    }
}
