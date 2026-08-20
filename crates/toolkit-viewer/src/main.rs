mod app;
mod follower;

use std::path::PathBuf;

use app::ViewerApp;
use eframe::egui;

fn main() -> eframe::Result {
    let initial_paths = std::env::args_os().skip(1).map(PathBuf::from).collect();
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Wgpu,
        viewport: egui::ViewportBuilder::default()
            .with_title("DeeBugee")
            .with_inner_size([1500.0, 900.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };

    eframe::run_native(
        "DeeBugee",
        options,
        Box::new(move |creation_context| {
            Ok(Box::new(ViewerApp::new(creation_context, initial_paths)))
        }),
    )
}
