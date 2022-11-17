use my_telemetry::MyTelemetryContext;

pub struct UploadQueue<T: Sync + Send + 'static> {
    pub queue: Option<Vec<(T, Option<MyTelemetryContext>)>>,
}

impl<T: Sync + Send + 'static> UploadQueue<T> {
    pub fn new() -> Self {
        Self { queue: None }
    }

    pub fn add<TIter: Iterator<Item = (T, Option<MyTelemetryContext>)>>(&mut self, dtos: TIter) {
        if self.queue.is_none() {
            self.queue = Some(Vec::new());
        }

        if let Some(queue) = self.queue.as_mut() {
            queue.extend(dtos);
        }
    }

    pub fn get_count(&self) -> usize {
        match &self.queue {
            Some(queue) => queue.len(),
            None => 0,
        }
    }

    pub fn get_items_to_upload(
        &mut self,
        max_amount: usize,
    ) -> Option<Vec<(T, Option<MyTelemetryContext>)>> {
        if self.queue.is_none() {
            return None;
        }

        if let Some(queue) = self.queue.as_mut() {
            if queue.len() > max_amount {
                let mut result = Vec::new();

                for _ in 0..max_amount {
                    result.push(queue.remove(0));
                }

                return Some(result);
            }
        }

        return self.queue.replace(Vec::new());
    }
}
