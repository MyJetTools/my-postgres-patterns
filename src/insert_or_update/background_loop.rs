use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use my_postgres::{InsertOrUpdateEntity, MyPostgres};
use rust_extensions::events_loop::EventsLoopTick;
use tokio::sync::Mutex;

use crate::upload_queue::UploadQueue;

pub struct InsertOrUpdateDbBackgroundLoop<TEntity: InsertOrUpdateEntity + Send + Sync + 'static> {
    upload_queue: Arc<(Mutex<UploadQueue<TEntity>>, AtomicUsize)>,
    max_operations_per_upload: usize,
    my_postgres: Arc<MyPostgres>,
    table_name: String,
    primary_key_name: String,
    max_attempts: usize,
}

impl<TEntity: InsertOrUpdateEntity + Send + Sync + 'static>
    InsertOrUpdateDbBackgroundLoop<TEntity>
{
    pub fn new(
        my_postgres: Arc<MyPostgres>,
        upload_queue: Arc<(Mutex<UploadQueue<TEntity>>, AtomicUsize)>,
        max_operations_per_upload: usize,
        table_name: String,
        primary_key_name: String,
        max_attempts: usize,
    ) -> Self {
        Self {
            upload_queue,
            max_operations_per_upload,
            my_postgres,
            table_name,
            primary_key_name,
            max_attempts,
        }
    }

    async fn get_operations_to_upload(
        &self,
    ) -> Option<Vec<(TEntity, Option<my_telemetry::MyTelemetryContext>)>> {
        let mut upload_queue = self.upload_queue.0.lock().await;
        let result = upload_queue.get_items_to_upload(self.max_operations_per_upload);
        self.upload_queue.1.store(
            upload_queue.get_count(),
            std::sync::atomic::Ordering::SeqCst,
        );

        result
    }
}

#[async_trait::async_trait]
impl<TEntity: InsertOrUpdateEntity + Send + Sync + 'static> EventsLoopTick<()>
    for InsertOrUpdateDbBackgroundLoop<TEntity>
{
    async fn tick(&self, _: ()) {
        if let Some(entities) = self.get_operations_to_upload().await {
            let attempt_no = 0;
            loop {
                let result = self
                    .my_postgres
                    .bulk_insert_or_update_db_entity(
                        entities.as_slice(),
                        &self.table_name,
                        &self.primary_key_name,
                    )
                    .await;

                if result.is_ok() {
                    break;
                }

                let err = result.err().unwrap();

                if attempt_no >= self.max_attempts {
                    println!(
                        "Max attempts reached for bulk insert if not exists . Error: {:?}",
                        err
                    );
                    break;
                }

                println!(
                    "Attempt no {}. Can not bulk insert if not exists entities. Error: {:?}",
                    attempt_no, err
                );

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
