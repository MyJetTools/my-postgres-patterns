use std::sync::{atomic::AtomicUsize, Arc};

use my_postgres::{InsertEntity, MyPostgres};
use my_telemetry::MyTelemetryContext;
use rust_extensions::{events_loop::EventsLoop, ApplicationStates};
use tokio::sync::Mutex;

use crate::{upload_queue::UploadQueue, InsertIfNotExistsDbBackgroundLoop};

pub struct InsertToDbOnBackground<TEntity: InsertEntity + Send + Sync + 'static> {
    upload_queue: Arc<(Mutex<UploadQueue<TEntity>>, AtomicUsize)>,
    events_loop: EventsLoop<()>,
}

impl<TEntity: InsertEntity + Send + Sync + 'static> InsertToDbOnBackground<TEntity> {
    pub async fn new(
        my_postgres: Arc<MyPostgres>,
        table_name: String,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        let result = Self {
            upload_queue: Arc::new((Mutex::new(UploadQueue::new()), AtomicUsize::new(0))),

            events_loop: EventsLoop::new("UploadToDbOnBackground".to_string()),
        };

        let background_loop = InsertIfNotExistsDbBackgroundLoop::new(
            my_postgres,
            result.upload_queue.clone(),
            table_name,
            15,
            5,
        );

        result
            .events_loop
            .register_event_loop(Arc::new(background_loop))
            .await;

        result
            .events_loop
            .start(app_states, my_logger::LOGGER.clone())
            .await;

        result
    }

    pub fn start_as_insert_if_not_exists(&self) {}

    fn update_count(&self, value: usize) {
        self.upload_queue
            .1
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_count(&self) -> usize {
        self.upload_queue
            .1
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn add<TIter: Iterator<Item = (TEntity, Option<MyTelemetryContext>)>>(
        &self,
        entities: TIter,
    ) {
        {
            let mut upload_queue = self.upload_queue.0.lock().await;
            upload_queue.add(entities);
            self.update_count(upload_queue.get_count());
        }

        self.events_loop.send(());
    }
}
