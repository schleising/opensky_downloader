use std::mem;

use mongodb::IndexModel;
use mongodb::{Client, Collection, Database};

use bson::doc;
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
        let uri: String = format!("mongodb://{}:27017", hostname);
        let client = Client::with_uri_str(&uri).await?;
        let database: Database = client.database(database_name);
        let collection: Collection<T> = database.collection(collection_name);

        Ok(DatabaseWriter {
            collection,
            chunk_size: DEFAULT_CHUNK_SIZE,
            records: Vec::with_capacity(DEFAULT_CHUNK_SIZE),
            join_handles: Vec::new(),
        })
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

    pub async fn finish(&mut self) -> Result<(), DatabaseError> {
        // Write the remaining records
        self.write_records();

        // Wait for all the tasks to finish
        for join_handle in self.join_handles.drain(..) {
            join_handle.await??;
        }

        Ok(())
    }
}
