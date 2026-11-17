use crate::connection_load_bloc::{ConnectionLoadBloc, Phase};
use eframe::egui::{self, Color32};

pub fn show(ui: &mut egui::Ui, bloc: &mut ConnectionLoadBloc) -> bool {
    let state = bloc.state();
    ui.heading("Тестування / Навантаження / З'єднання");
    ui.colored_label(
        Color32::YELLOW,
        "Тест створює реальне навантаження на сервер і клієнт без обмеження кількості з'єднань.",
    );
    let phase = state.phase;
    let elapsed = if phase == Phase::Running || phase == Phase::Stopping {
        state
            .started
            .map_or(state.elapsed, |started| started.elapsed())
    } else {
        state.elapsed
    };
    ui.label(format!(
        "Стан: {}",
        match phase {
            Phase::Idle => "не запущено",
            Phase::Running => "працює",
            Phase::Stopping => "зупиняється",
            Phase::Stopped => "зупинено",
        }
    ));
    if phase != Phase::Idle {
        ui.label(format!(
            "Сервер: {} / транспорт: {}",
            state.target, state.transport
        ));
        ui.label(format!(
            "Час: {:.1} с / кроків: {}",
            elapsed.as_secs_f64(),
            state.steps
        ));
        ui.label(format!(
            "Спроб: {} / підключаються: {} / активні: {} / максимум: {}",
            state.attempted, state.connecting, state.connected, state.peak
        ));
        ui.label(format!(
            "Помилки підключення: {} / ping успішно: {} / ping помилок: {}",
            state.failed_connections, state.ping_ok, state.ping_failed
        ));
        ui.label(format!(
            "Скасовано підключень: {} / закриваються: {} / закрито: {} / помилки закриття: {}",
            state.cancelled, state.closing, state.closed, state.close_failed
        ));
        if !state.errors.is_empty() {
            ui.label("Останні помилки:");
            for error in state.errors.iter().rev() {
                ui.colored_label(Color32::LIGHT_RED, error);
            }
        }
    }
    drop(state);
    let mut settings = bloc.settings();
    let editable = matches!(phase, Phase::Idle | Phase::Stopped);
    ui.add_enabled_ui(editable, |ui| {
        ui.horizontal(|ui| {
            ui.label("З'єднань на крок");
            ui.add(egui::DragValue::new(&mut settings.step_size).range(1..=u32::MAX));
        });
        ui.horizontal(|ui| {
            ui.label("Інтервал кроку (с)");
            ui.add(egui::DragValue::new(&mut settings.step_interval_seconds).range(1..=u32::MAX));
        });
        ui.horizontal(|ui| {
            ui.label("Інтервал ping (с)");
            ui.add(egui::DragValue::new(&mut settings.ping_interval_seconds).range(1..=u32::MAX));
        });
    });
    if settings != bloc.settings()
        && let Err(error) = bloc.set_settings(settings)
    {
        ui.colored_label(
            Color32::LIGHT_RED,
            format!("Клієнт / налаштування: {error:#}"),
        );
    }
    ui.small("Параметри застосовуються при запуску. Невдалі з'єднання закриваються без повтору.");
    ui.small("Локальний тайм-аут означає рішення клієнта припинити очікування, а не доведену причину збою. Транспортні помилки без підтвердженого джерела позначаються як «Сторона невідома».");
    if phase == Phase::Running && ui.button("Зупинити").clicked() {
        bloc.stop();
    }
    editable && ui.button("Запустити").clicked()
}
