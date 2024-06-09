mod downloader;
mod models;

use downloader::download;

fn main() {
    let url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv";

    match download(url) {
        Ok(_) => println!("Downloaded successfully"),
        Err(e) => eprintln!("Error: {}", e),
    }
}
