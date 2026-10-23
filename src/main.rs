mod ui;

fn main() -> eframe::Result {
    eframe::run_native(
        "JSON-RPC-2 | Desktop Client Lab",
        eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default()
                .with_inner_size([1280.0, 860.0])
                .with_min_inner_size([960.0, 640.0]),
            ..Default::default()
        },
        Box::new(|context| Ok(Box::new(ui::DesktopApp::new(context)?))),
    )
}
