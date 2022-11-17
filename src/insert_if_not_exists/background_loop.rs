use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use my_postgres::{InsertEntity, MyPostgres};
use rust_extensions::events_loop::EventsLoopTick;
use tokio::sync::Mutex;

use crate::upload_queue::UploadQueue;

pub struct InsertIfNotExistsDbBackgroundLoop<TEntity: InsertEntity + Send + Sync + 'static> {
    upload_queue: Arc<(Mutex<UploadQueue<TEntity>>, AtomicUsize)>,
    max_operations_per_upload: usize,
    my_postgres: Arc<MyPostgres>,
    max_attempts: usize,
    table_name: String,
}

impl<TEntity: InsertEntity + Send + Sync + 'static> InsertIfNotExistsDbBackgroundLoop<TEntity> {
    pub fn new(
        my_postgres: Arc<MyPostgres>,
        upload_queue: Arc<(Mutex<UploadQueue<TEntity>>, AtomicUsize)>,
        table_name: String,
        max_operations_per_upload: usize,
        max_attempts: usize,
    ) -> Self {
        Self {
            upload_queue,
            max_operations_per_upload,
            my_postgres,
            max_attempts,
            table_name,
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
impl<TEntity: InsertEntity + Send + Sync + 'static> EventsLoopTick<()>
    for InsertIfNotExistsDbBackgroundLoop<TEntity>
{
    async fn tick(&self, _: ()) {
        if let Some(entities) = self.get_operations_to_upload().await {
            let attempt_no = 0;
            loop {
                let result = self
                    .my_postgres
                    .bulk_insert_db_entities_if_not_exists(entities.as_slice(), &self.table_name)
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
