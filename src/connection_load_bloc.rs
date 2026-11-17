use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Result;
use json_rpc_2_tester::connection::{
    GatewaySettings, LoadConnectError, connect_gateway_test_transport,
};
use tokio::{
    runtime::Runtime,
    sync::watch,
    task::{JoinHandle, JoinSet},
};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_ERRORS: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadSettings {
    pub step_size: u32,
    pub step_interval_seconds: u32,
    pub ping_interval_seconds: u32,
}

impl Default for LoadSettings {
    fn default() -> Self {
        Self {
            step_size: 1000,
            step_interval_seconds: 5,
            ping_interval_seconds: 4,
        }
    }
}

impl LoadSettings {
    fn validate(self) -> Result<()> {
        anyhow::ensure!(
            self.step_size > 0,
            "Кількість з'єднань у кроці має бути більшою за нуль"
        );
        anyhow::ensure!(
            self.step_interval_seconds > 0 && self.ping_interval_seconds > 0,
            "Інтервали кроку та ping мають бути більшими за нуль"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Running,
    Stopping,
    Stopped,
}

pub struct State {
    pub phase: Phase,
    pub target: String,
    pub transport: String,
    pub started: Option<Instant>,
    pub elapsed: Duration,
    pub steps: u64,
    pub attempted: u64,
    pub connecting: u64,
    pub cancelled: u64,
    pub connected: u64,
    pub closing: u64,
    pub peak: u64,
    pub failed_connections: u64,
    pub ping_ok: u64,
    pub ping_failed: u64,
    pub closed: u64,
    pub close_failed: u64,
    pub errors: VecDeque<String>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            phase: Phase::Idle,
            target: String::new(),
            transport: String::new(),
            started: None,
            elapsed: Duration::ZERO,
            steps: 0,
            attempted: 0,
            connecting: 0,
            cancelled: 0,
            connected: 0,
            closing: 0,
            peak: 0,
            failed_connections: 0,
            ping_ok: 0,
            ping_failed: 0,
            closed: 0,
            close_failed: 0,
            errors: VecDeque::new(),
        }
    }
}

impl State {
    fn error(&mut self, message: String) {
        if self.errors.len() == MAX_ERRORS {
            self.errors.pop_front();
        }
        self.errors.push_back(message);
    }
}

fn connection_error_message(error: &LoadConnectError) -> String {
    match error {
        LoadConnectError::Client(_) => format!("Клієнт / з'єднання: {error}"),
        LoadConnectError::Timeout { .. } => {
            format!("Клієнт / з'єднання: {error}; причина затримки невідома")
        }
        LoadConnectError::Transport(_) => {
            format!("Сторона невідома / з'єднання: {error}")
        }
    }
}

pub struct ConnectionLoadBloc {
    state: Arc<Mutex<State>>,
    settings: LoadSettings,
    stop: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl Default for ConnectionLoadBloc {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            settings: LoadSettings::default(),
            stop: None,
            task: None,
        }
    }
}

impl ConnectionLoadBloc {
    pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("load test state lock poisoned")
    }

    pub fn settings(&self) -> LoadSettings {
        self.settings
    }

    pub fn set_settings(&mut self, settings: LoadSettings) -> Result<()> {
        anyhow::ensure!(
            matches!(self.state().phase, Phase::Idle | Phase::Stopped),
            "Налаштування можна змінювати лише перед запуском тесту"
        );
        settings.validate()?;
        self.settings = settings;
        Ok(())
    }

    pub fn start(
        &mut self,
        runtime: &Runtime,
        settings: GatewaySettings,
        kind: &str,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(kind, "tcp" | "tls" | "quic"),
            "Unsupported transport: {kind}"
        );
        settings.validate()?;
        self.settings.validate()?;
        anyhow::ensure!(
            matches!(self.state().phase, Phase::Idle | Phase::Stopped),
            "Test is already running"
        );
        let target = format!("{}:{}", settings.host, settings.port);
        *self.state() = State {
            phase: Phase::Running,
            target,
            transport: kind.to_owned(),
            started: Some(Instant::now()),
            ..State::default()
        };
        let (stop, receiver) = watch::channel(false);
        self.stop = Some(stop);
        self.task = Some(runtime.spawn(run(
            self.state.clone(),
            settings,
            kind.to_owned(),
            receiver,
            self.settings,
        )));
        Ok(())
    }

    pub fn stop(&mut self) {
        if self.state().phase == Phase::Running {
            self.state().phase = Phase::Stopping;
            if let Some(stop) = &self.stop {
                let _ = stop.send(true);
            }
        }
    }

    pub fn poll(&mut self, runtime: &Runtime) {
        if self.task.as_ref().is_some_and(JoinHandle::is_finished) {
            let task = self.task.take().expect("finished task exists");
            if let Err(error) = runtime.block_on(task) {
                let mut state = self.state();
                state.error(format!(
                    "Клієнт / тест з'єднань аварійно завершився: {error}"
                ));
                state.phase = Phase::Stopped;
                state.connecting = 0;
                state.connected = 0;
                state.closing = 0;
                state.elapsed = state
                    .started
                    .map_or(Duration::ZERO, |started| started.elapsed());
            }
        }
    }

    pub async fn shutdown(&mut self) {
        self.stop();
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            eprintln!("Load-test task failed: {error}");
        }
    }
}

async fn run(
    state: Arc<Mutex<State>>,
    settings: GatewaySettings,
    kind: String,
    mut stop: watch::Receiver<bool>,
    load: LoadSettings,
) {
    let mut tasks = JoinSet::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(load.step_interval_seconds.into()));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { break; }
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    let mut state = state.lock().expect("load test state lock poisoned");
                    state.error(format!("Клієнт / задача з'єднання завершилась з помилкою: {error}"));
                }
            }
            _ = ticker.tick() => {
                {
                    let mut state = state.lock().expect("load test state lock poisoned");
                    state.steps += 1;
                    state.attempted += u64::from(load.step_size);
                    state.connecting += u64::from(load.step_size);
                }
                for _ in 0..load.step_size {
                    tasks.spawn(connection_task(state.clone(), settings.clone(), kind.clone(), stop.clone(), Duration::from_secs(load.ping_interval_seconds.into())));
                }
            }
        }
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            state
                .lock()
                .expect("load test state lock poisoned")
                .error(format!(
                    "Клієнт / задача з'єднання завершилась з помилкою: {error}"
                ));
        }
    }
    let mut state = state.lock().expect("load test state lock poisoned");
    state.phase = Phase::Stopped;
    state.elapsed = state
        .started
        .map_or(Duration::ZERO, |started| started.elapsed());
}

async fn connection_task(
    state: Arc<Mutex<State>>,
    settings: GatewaySettings,
    kind: String,
    mut stop: watch::Receiver<bool>,
    ping_interval: Duration,
) {
    let connection = tokio::select! {
        biased;
        _ = stop.changed() => {
            let mut state = state.lock().expect("load test state lock poisoned");
            state.connecting -= 1;
            state.cancelled += 1;
            return;
        }
        result = connect_gateway_test_transport(&settings, &kind) => result,
    };
    let transport = match connection {
        Ok(transport) => {
            let mut state = state.lock().expect("load test state lock poisoned");
            state.connecting -= 1;
            state.connected += 1;
            state.peak = state.peak.max(state.connected);
            transport
        }
        Err(error) => {
            let mut state = state.lock().expect("load test state lock poisoned");
            state.connecting -= 1;
            state.failed_connections += 1;
            state.error(connection_error_message(&error));
            return;
        }
    };
    let mut ticker = tokio::time::interval(ping_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = ticker.tick() => {
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = tokio::time::timeout(Duration::from_millis(settings.connect_timeout_ms), transport.transport_ping()) => result,
                };
                let mut state = state.lock().expect("load test state lock poisoned");
                match result {
                    Ok(Ok(())) => state.ping_ok += 1,
                    Ok(Err(error)) => {
                        state.ping_failed += 1;
                        state.error(format!("Сторона невідома / ping: {error}"));
                        break;
                    }
                    Err(_) => {
                        state.ping_failed += 1;
                        state.error(format!("Клієнт / ping: перевищено тайм-аут ({} мс); причина затримки невідома", settings.connect_timeout_ms));
                        break;
                    }
                }
            }
        }
    }
    {
        let mut state = state.lock().expect("load test state lock poisoned");
        state.connected -= 1;
        state.closing += 1;
    }
    let close = tokio::time::timeout(CLOSE_TIMEOUT, transport.close()).await;
    let mut state = state.lock().expect("load test state lock poisoned");
    state.closing -= 1;
    match close {
        Ok(Ok(())) => state.closed += 1,
        Ok(Err(error)) => {
            state.close_failed += 1;
            state.error(format!("Сторона невідома / закриття: {error}"));
        }
        Err(_) => {
            state.close_failed += 1;
            state.error(format!(
                "Клієнт / закриття: перевищено тайм-аут ({} мс); причина затримки невідома",
                CLOSE_TIMEOUT.as_millis()
            ));
        }
    }
}

#[cfg(test)]
#[path = "connection_load_bloc/tests.rs"]
mod tests;
