use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

type AsyncFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct TimerEntry {
    start: Instant,
    handle: tokio::task::JoinHandle<()>,
    func: AsyncFn,
}

pub struct Debouncer {
    timers: Arc<Mutex<HashMap<String, TimerEntry>>>,
    running_executions: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl Debouncer {
    pub fn new() -> Self {
        Self {
            timers: Arc::new(Mutex::new(HashMap::new())),
            running_executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn debounce<F, Fut>(&self, id: &str, func: F, debounce_ms: u64, max_debounce_ms: u64)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut timers = self.timers.lock().await;

        let start = timers
            .get(id)
            .map(|entry| entry.start)
            .unwrap_or_else(Instant::now);

        if let Some(old) = timers.remove(id) {
            old.handle.abort();
        }

        let func_arc: AsyncFn = Arc::new(move || Box::pin(func()));

        if debounce_ms == 0 {
            drop(timers);
            self.run(id, func_arc).await;
            return;
        }

        if start.elapsed() >= Duration::from_millis(max_debounce_ms) {
            drop(timers);
            self.run(id, func_arc).await;
            return;
        }

        let id_owned = id.to_string();
        let timers_ref = self.timers.clone();
        let running_ref = self.running_executions.clone();
        let func_clone = func_arc.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(debounce_ms)).await;

            {
                let mut timers = timers_ref.lock().await;
                timers.remove(&id_owned);
            }

            let lock = {
                let mut running = running_ref.lock().await;
                running
                    .entry(id_owned.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            };

            let _guard = lock.lock().await;
            (func_clone)().await;

            {
                let mut running = running_ref.lock().await;
                running.remove(&id_owned);
            }
        });

        timers.insert(
            id.to_string(),
            TimerEntry {
                start,
                handle,
                func: func_arc,
            },
        );
    }

    async fn run(&self, id: &str, func: AsyncFn) {
        let lock = {
            let mut running = self.running_executions.lock().await;
            running
                .entry(id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        let _guard = lock.lock().await;

        {
            let mut timers = self.timers.lock().await;
            timers.remove(id);
        }

        func().await;

        {
            let mut running = self.running_executions.lock().await;
            running.remove(id);
        }
    }

    pub async fn execute_now(&self, id: &str) {
        let func = {
            let mut timers = self.timers.lock().await;
            if let Some(entry) = timers.remove(id) {
                entry.handle.abort();
                Some(entry.func)
            } else {
                None
            }
        };

        if let Some(func) = func {
            self.run(id, func).await;
        }
    }

    pub async fn is_debounced(&self, id: &str) -> bool {
        let timers = self.timers.lock().await;
        timers.contains_key(id)
    }

    pub async fn is_currently_executing(&self, id: &str) -> bool {
        let running = self.running_executions.lock().await;
        running.contains_key(id)
    }
}

impl Default for Debouncer {
    fn default() -> Self {
        Self::new()
    }
}
