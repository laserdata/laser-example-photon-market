use laser_sdk::prelude::{AgentHandle, LaserError};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout_at};
use tracing::{info, warn};

pub const HANDLER_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const SERVICE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

pub struct ServiceHandle {
    name: &'static str,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    dependencies: Vec<ServiceHandle>,
    shutdown_timeout: Duration,
    failures: Arc<Mutex<Vec<String>>>,
}

impl ServiceHandle {
    pub fn new(name: &'static str) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            name,
            shutdown,
            tasks: Vec::new(),
            dependencies: Vec::new(),
            shutdown_timeout: SERVICE_SHUTDOWN_TIMEOUT,
            failures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn watch(&self) -> ShutdownWatch {
        ShutdownWatch {
            signal: self.shutdown.subscribe(),
        }
    }

    pub fn track(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    pub fn track_result<F>(&mut self, task: F)
    where
        F: Future<Output = Result<(), LaserError>> + Send + 'static,
    {
        let service = self.name;
        let failures = self.failures.clone();
        self.track(tokio::spawn(async move {
            if let Err(error) = task.await {
                warn!("Service '{service}' background task failed. {error}");
                failures
                    .lock()
                    .expect("service failure lock is not poisoned")
                    .push(error.to_string());
            }
        }));
    }

    /// Attach a ready agent to this service's shutdown boundary.
    pub async fn track_agent(&mut self, mut agent: AgentHandle) -> Result<(), LaserError> {
        agent.ready().await?;
        let service = self.name;
        let failures = self.failures.clone();
        let mut shutdown = self.watch();
        self.track(tokio::spawn(async move {
            shutdown.cancelled().await;
            if let Err(error) = agent.shutdown().await {
                warn!("Service '{service}' agent did not drain cleanly. {error}");
                failures
                    .lock()
                    .expect("service failure lock is not poisoned")
                    .push(error.to_string());
            }
        }));
        Ok(())
    }

    /// Attach a service that this handle's draining handlers still call. It is
    /// signalled only after this handle's own tasks finish, so an in-flight
    /// handler completes its round-trip instead of waiting out its deadline
    /// against an already-stopped counterparty.
    pub fn track_dependency(&mut self, dependency: ServiceHandle) {
        self.dependencies.push(dependency);
    }

    pub async fn shutdown(mut self) -> Result<(), LaserError> {
        let _ = self.shutdown.send(true);
        let service = self.name;
        let deadline = Instant::now() + self.shutdown_timeout;
        let mut failures = Vec::new();
        for mut task in self.tasks.drain(..) {
            match timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failures.push("drain deadline elapsed".to_owned());
                }
            }
        }
        for dependency in self.dependencies.drain(..) {
            if let Err(error) = Box::pin(dependency.shutdown()).await {
                failures.push(error.to_string());
            }
        }
        failures.extend(
            self.failures
                .lock()
                .expect("service failure lock is not poisoned")
                .drain(..),
        );
        if failures.is_empty() {
            info!("Service '{service}' stopped cleanly");
            Ok(())
        } else {
            let failures = failures.join(", ");
            warn!("Service '{service}' stopped with incomplete work. {failures}");
            Err(LaserError::Invalid(format!(
                "service '{service}' shutdown was incomplete: {failures}"
            )))
        }
    }
}

#[derive(Clone)]
pub struct ShutdownWatch {
    signal: watch::Receiver<bool>,
}

impl ShutdownWatch {
    pub async fn cancelled(&mut self) {
        while !*self.signal.borrow() {
            if self.signal.changed().await.is_err() {
                break;
            }
        }
    }
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            warn!("failed to listen for ctrl-c");
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install the SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}

pub async fn eventually<F, Fut>(timeout: Duration, mut probe: F) -> Result<(), LaserError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if probe().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(
                "condition was not met before the deadline".to_owned(),
            ));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn given_a_condition_that_becomes_true_when_awaited_then_should_return_ok() {
        let count = Arc::new(AtomicU32::new(0));
        let result = eventually(Duration::from_secs(1), || {
            let observed = count.fetch_add(1, Ordering::SeqCst);
            async move { observed >= 2 }
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn given_a_condition_that_never_holds_when_awaited_then_should_time_out() {
        let result = eventually(Duration::from_millis(50), || async { false }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_cooperative_work_when_shutdown_then_should_wait_for_cleanup() {
        let cleaned = Arc::new(AtomicU32::new(0));
        let mut service = ServiceHandle::new("test");
        let mut shutdown = service.watch();
        let observed = cleaned.clone();
        service.track(tokio::spawn(async move {
            shutdown.cancelled().await;
            sleep(Duration::from_millis(10)).await;
            observed.store(1, Ordering::SeqCst);
        }));

        assert!(service.shutdown().await.is_ok());
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_a_dependency_when_shutdown_then_should_stop_it_after_the_own_drain() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut service = ServiceHandle::new("test");
        let mut dependency = ServiceHandle::new("test-dependency");

        let mut cancelled = dependency.watch();
        let observed = order.clone();
        dependency.track(tokio::spawn(async move {
            cancelled.cancelled().await;
            observed
                .lock()
                .expect("order lock is not poisoned")
                .push("dependency");
        }));

        let mut cancelled = service.watch();
        let observed = order.clone();
        service.track(tokio::spawn(async move {
            cancelled.cancelled().await;
            sleep(Duration::from_millis(10)).await;
            observed
                .lock()
                .expect("order lock is not poisoned")
                .push("service");
        }));
        service.track_dependency(dependency);

        assert!(service.shutdown().await.is_ok());
        let order = order.lock().expect("order lock is not poisoned");
        assert_eq!(*order, ["service", "dependency"]);
    }

    #[tokio::test]
    async fn given_failed_background_work_when_shutdown_then_should_report_the_error() {
        let mut service = ServiceHandle::new("test");
        service
            .track_result(async { Err(LaserError::Invalid("consumer commit failed".to_owned())) });

        let error = service
            .shutdown()
            .await
            .expect_err("a background failure makes shutdown fail");
        match error {
            LaserError::Invalid(message) => assert_eq!(
                message,
                "service 'test' shutdown was incomplete: invalid: consumer commit failed"
            ),
            other => panic!("expected an invalid error, got {other}"),
        }
    }

    #[tokio::test]
    async fn given_stuck_work_when_shutdown_then_should_abort_at_the_deadline() {
        let mut service =
            ServiceHandle::new("test").with_shutdown_timeout(Duration::from_millis(20));
        service.track(tokio::spawn(std::future::pending()));

        let started = Instant::now();
        assert!(service.shutdown().await.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
