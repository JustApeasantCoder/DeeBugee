#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

mod app;
mod follower;

use std::{ffi::OsString, path::PathBuf};

use app::{LaunchRequest, ViewerApp};
use eframe::egui;

fn main() -> eframe::Result {
    let launch = parse_launch_request(std::env::args_os().skip(1).collect());
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Wgpu,
        viewport: egui::ViewportBuilder::default()
            .with_title(launch.window_title())
            .with_inner_size([1500.0, 900.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };

    eframe::run_native(
        "DeeBugee",
        options,
        Box::new(move |creation_context| Ok(Box::new(ViewerApp::new(creation_context, launch)))),
    )
}

fn parse_launch_request(args: Vec<OsString>) -> LaunchRequest {
    let mut workspace_path = None;
    let mut log_paths = Vec::new();
    let mut arguments = args.into_iter();

    while let Some(argument) = arguments.next() {
        if argument == "--workspace" {
            if let Some(path) = arguments.next() {
                workspace_path = Some(PathBuf::from(path));
            }
        } else if argument == "--logs" {
            if let Some(path) = arguments.next() {
                log_paths.push(PathBuf::from(path));
            }
        } else {
            // Keep the original `dee-bugee.exe path-to-log.jsonl` contract.
            log_paths.push(PathBuf::from(argument));
        }
    }

    LaunchRequest::new(workspace_path, log_paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_and_log_arguments_are_parsed_without_affecting_legacy_paths() {
        let request = parse_launch_request(vec![
            "--workspace".into(),
            "C:/work/project-a/.deebugee/workspace.toml".into(),
            "--logs".into(),
            "C:/logs/project-a.jsonl".into(),
            "C:/logs/sidecar.jsonl".into(),
        ]);

        assert_eq!(
            request.workspace_path,
            Some(PathBuf::from("C:/work/project-a/.deebugee/workspace.toml"))
        );
        assert_eq!(
            request.log_paths,
            vec![
                PathBuf::from("C:/logs/project-a.jsonl"),
                PathBuf::from("C:/logs/sidecar.jsonl")
            ]
        );
    }
}
