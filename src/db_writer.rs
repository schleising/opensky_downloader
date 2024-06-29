use std::mem;

use bson::doc;
use mongodb::IndexModel;
use mongodb::{Client, Collection, Database};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::{spawn, JoinError, JoinHandle};

const DEFAULT_CHUNK_SIZE: usize = 1000;

#[derive(Debug)]
pub enum DatabaseError {
    MongoError(mongodb::error::Error),
    JoinError(JoinError),
}

impl From<mongodb::error::Error> for DatabaseError {
    fn from(error: mongodb::error::Error) -> Self {
        DatabaseError::MongoError(error)
    }
}

impl From<JoinError> for DatabaseError {
    fn from(error: JoinError) -> Self {
        DatabaseError::JoinError(error)
    }
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatabaseError::MongoError(error) => write!(f, "MongoDB error: {}", error),
            DatabaseError::JoinError(error) => write!(f, "Join error: {}", error),
        }
    }
}

pub struct DatabaseWriter<T> {
    collection: Collection<T>,
    chunk_size: usize,
    records: Vec<T>,
    join_handles: Vec<JoinHandle<Result<(), DatabaseError>>>,
}

impl<T> DatabaseWriter<T>
where
    T: Send + Sync + serde::Serialize + 'static,
{
    pub async fn new(
        hostname: &str,
        database_name: &str,
        collection_name: &str,
    ) -> Result<Self, DatabaseError> {
        // Construct the URI for the MongoDB connection
        let uri: String = format!(
            "mongodb://{}:27017/?serverSelectionTimeoutMS=2000",
            hostname
        );
        let client = Client::with_uri_str(&uri).await?;
        let database: Database = client.database(database_name);
        let collection: Collection<T> = database.collection(collection_name);

        let db_writer = Ok(DatabaseWriter {
            collection,
            chunk_size: DEFAULT_CHUNK_SIZE,
            records: Vec::with_capacity(DEFAULT_CHUNK_SIZE),
            join_handles: Vec::new(),
        });

        // Ping the server to check if the connection is successful
        database.run_command(doc! { "ping": 1 }, None).await?;

        // Return the database writer
        db_writer
    }

    #[allow(dead_code)]
    pub fn set_chunk_size(&mut self, chunk_size: usize) {
        // Set the chunk size
        self.chunk_size = chunk_size;

        // Create a new vector with the new capacity
        self.records = Vec::with_capacity(chunk_size);
    }

    pub async fn drop_collection(&self) -> Result<(), DatabaseError> {
        self.collection.drop(None).await?;
        Ok(())
    }

    pub async fn create_index(&self, field: &str) -> Result<(), DatabaseError> {
        let model: IndexModel = IndexModel::builder().keys(doc! { field: 1 }).build();
        self.collection.create_index(model, None).await?;
        Ok(())
    }

    fn write_records(&mut self) {
        // Create a new vector and take the old one, using mem::replace to avoid a clone
        let records_vec = mem::replace(&mut self.records, Vec::with_capacity(self.chunk_size));

        // Clone the collection
        let collection = self.collection.clone();

        // Spawn a new task to insert the records
        self.join_handles.push(spawn(async move {
            // Insert the aircraft into the collection
            collection.insert_many(records_vec, None).await?;

            // Return Ok
            Ok(())
        }));
    }

    pub fn add_record(&mut self, record: T) {
        self.records.push(record);

        if self.records.len() >= self.chunk_size {
            self.write_records();
        }
    }

    pub fn finish(&mut self) -> UnboundedReceiver<f64> {
        // Write the remaining records
        self.write_records();

        // Get the join handles into a new vector
        let mut join_handles = mem::take(&mut self.join_handles);

        // Create a channel to wait for the tasks to finish
        let (tx, rx) = unbounded_channel::<f64>();

        // Spawn a new task to wait for all the tasks to finish
        spawn(async move {
            // Get the number of tasks
            let tasks = join_handles.len() as u64;

            // Initialise a counter
            let mut counter: u64 = 0;

            // Wait for all the tasks to finish
            for join_handle in join_handles.drain(..) {
                match join_handle.await {
                    Ok(_) => {
                        // Increment the counter
                        counter += 1;

                        // Calculate the percentage complete
                        let percentage = (counter as f64 / tasks as f64) * 100.0;

                        // Send the percentage complete
                        let _ = tx.send(percentage);
                    }
                    Err(_) => {}
                }
            }

            // Send OK to close the receiver
            let _ = tx.send(100.0);
        });

        // Return the receiver
        rx
    }
}
