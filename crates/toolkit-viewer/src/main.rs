#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

mod app;
mod follower;
#[allow(dead_code)]
mod update;

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use app::{LaunchRequest, ProjectConfiguration, ViewerApp, configure_project};
use eframe::egui;

fn main() -> eframe::Result {
    let launch = match parse_startup_request(std::env::args_os().skip(1).collect()) {
        Ok(StartupRequest::Launch(launch)) => launch,
        Ok(StartupRequest::Configure(configuration)) => {
            match configure_project(configuration) {
                Ok(manifest_path) => {
                    println!("Created project configuration: {}", manifest_path.display())
                }
                Err(error) => command_failure(format!("Unable to configure project: {error}")),
            }
            return Ok(());
        }
        Err(error) => command_failure(error),
    };
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Wgpu,
        viewport: egui::ViewportBuilder::default()
            .with_title(launch.window_title())
            .with_inner_size([1500.0, 900.0])
            .with_min_inner_size([900.0, 600.0])
            .with_icon(application_icon()),
        ..Default::default()
    };

    eframe::run_native(
        "DeeBugee",
        options,
        Box::new(move |creation_context| Ok(Box::new(ViewerApp::new(creation_context, launch)))),
    )
}

fn application_icon() -> Arc<egui::IconData> {
    Arc::new(
        eframe::icon_data::from_png_bytes(include_bytes!("../../../assets/deebugee-logo.png"))
            .expect("embedded DeeBugee logo must be a valid PNG"),
    )
}

fn command_failure(error: String) -> ! {
    eprintln!("{error}");
    std::process::exit(2);
}

enum StartupRequest {
    Launch(LaunchRequest),
    Configure(ProjectConfiguration),
}

fn parse_startup_request(args: Vec<OsString>) -> Result<StartupRequest, String> {
    if args
        .first()
        .is_some_and(|argument| argument == "--configure-project")
    {
        parse_project_configuration(args).map(StartupRequest::Configure)
    } else {
        Ok(StartupRequest::Launch(parse_launch_request(args)))
    }
}

fn parse_project_configuration(args: Vec<OsString>) -> Result<ProjectConfiguration, String> {
    let mut arguments = args.into_iter();
    let _command = arguments.next();
    let root = arguments.next().map(PathBuf::from).ok_or_else(|| {
        "Usage: dee-bugee.exe --configure-project <root> --project-id <id> --project-name <name> --source <path> [--source <path> ...] [--force]".to_string()
    })?;
    let mut id = None;
    let mut name = None;
    let mut sources = Vec::new();
    let mut overwrite = false;

    while let Some(argument) = arguments.next() {
        match argument.to_string_lossy().as_ref() {
            "--project-id" => id = Some(next_configuration_value(&mut arguments, "--project-id")?),
            "--project-name" => {
                name = Some(next_configuration_value(&mut arguments, "--project-name")?)
            }
            "--source" => sources.push(next_configuration_value(&mut arguments, "--source")?),
            "--force" => overwrite = true,
            unknown => return Err(format!("Unknown --configure-project option: {unknown}")),
        }
    }

    Ok(ProjectConfiguration {
        root,
        id: id.ok_or_else(|| "--configure-project requires --project-id <id>".to_string())?,
        name: name
            .ok_or_else(|| "--configure-project requires --project-name <name>".to_string())?,
        sources,
        overwrite,
    })
}

fn next_configuration_value(
    arguments: &mut impl Iterator<Item = OsString>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| format!("--configure-project requires a value after {option}"))
}

fn parse_launch_request(args: Vec<OsString>) -> LaunchRequest {
    let mut workspace_path = None;
    let mut project_root = None;
    let mut log_paths = Vec::new();
    let mut arguments = args.into_iter();

    while let Some(argument) = arguments.next() {
        if argument == "--workspace" {
            if let Some(path) = arguments.next() {
                workspace_path = Some(PathBuf::from(path));
            }
        } else if argument == "--project" {
            if let Some(path) = arguments.next() {
                project_root = Some(PathBuf::from(path));
            }
        } else if argument == "--logs" {
            if let Some(path) = arguments.next() {
                log_paths.push(PathBuf::from(path));
            }
        } else {
            let path = PathBuf::from(argument);
            if project_root.is_none() && is_project_path(&path) {
                project_root = Some(path);
            } else {
                // Keep the original `dee-bugee.exe path-to-log.jsonl` contract.
                log_paths.push(path);
            }
        }
    }

    LaunchRequest::new(workspace_path, project_root, log_paths)
}

fn is_project_path(path: &std::path::Path) -> bool {
    path.join(".deebugee").join("project.toml").is_file()
        || (path.is_file()
            && path.file_name().is_some_and(|name| name == "project.toml")
            && path
                .parent()
                .and_then(|parent| parent.file_name())
                .is_some_and(|name| name == ".deebugee"))
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
        assert_eq!(request.project_root, None);
        assert_eq!(
            request.log_paths,
            vec![
                PathBuf::from("C:/logs/project-a.jsonl"),
                PathBuf::from("C:/logs/sidecar.jsonl")
            ]
        );
    }

    #[test]
    fn project_argument_is_kept_separate_from_log_paths() {
        let request = parse_launch_request(vec![
            "--project".into(),
            "C:/work/project-a".into(),
            "--logs".into(),
            "C:/logs/override.jsonl".into(),
        ]);

        assert_eq!(request.workspace_path, None);
        assert_eq!(
            request.project_root,
            Some(PathBuf::from("C:/work/project-a"))
        );
        assert_eq!(
            request.log_paths,
            vec![PathBuf::from("C:/logs/override.jsonl")]
        );
    }

    #[test]
    fn positional_project_root_is_discovered_from_its_manifest() {
        let root =
            std::env::temp_dir().join(format!("dee-bugee-project-cli-{}", std::process::id()));
        let manifest_directory = root.join(".deebugee");
        std::fs::create_dir_all(&manifest_directory).unwrap();
        std::fs::write(manifest_directory.join("project.toml"), "version = 1\n").unwrap();

        let request = parse_launch_request(vec![root.clone().into_os_string()]);
        assert_eq!(request.project_root, Some(root.clone()));
        assert!(request.log_paths.is_empty());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn configure_project_arguments_are_parsed_without_launching_the_viewer() {
        let request = parse_startup_request(vec![
            "--configure-project".into(),
            "C:/work/project-a".into(),
            "--project-id".into(),
            "com.example.project-a".into(),
            "--project-name".into(),
            "Project A".into(),
            "--source".into(),
            "%LOCALAPPDATA%/ProjectA/logs".into(),
            "--source".into(),
            "logs/development".into(),
            "--force".into(),
        ])
        .unwrap();

        let StartupRequest::Configure(request) = request else {
            panic!("expected a configure request");
        };
        assert_eq!(request.id, "com.example.project-a");
        assert_eq!(request.name, "Project A");
        assert_eq!(
            request.sources,
            ["%LOCALAPPDATA%/ProjectA/logs", "logs/development"]
        );
        assert!(request.overwrite);
    }
}
