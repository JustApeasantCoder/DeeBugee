use std::{
    ffi::OsString,
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc::{self, Receiver},
    thread,
};

use reqwest::{
    blocking::Client,
    header::{ACCEPT, USER_AGENT},
};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const REPOSITORY: &str = "JustApeasantCoder/DeeBugee";
const API_VERSION: &str = "2022-11-28";
const VIEWER_ASSET: &str = "dee-bugee.exe";
const UPDATER_ASSET: &str = "dee-bugee-updater.exe";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableUpdate {
    pub version: Version,
    pub tag_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckResult {
    Current,
    Available(AvailableUpdate),
    Failed(String),
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
}

impl Release {
    fn asset(&self, name: &str) -> Result<&ReleaseAsset, String> {
        let assets: Vec<&ReleaseAsset> = self
            .assets
            .iter()
            .filter(|asset| asset.name == name)
            .collect();
        match assets.as_slice() {
            [asset] => Ok(asset),
            [] => Err(format!("Release {} does not contain {name}", self.tag_name)),
            _ => Err(format!(
                "Release {} contains more than one {name}",
                self.tag_name
            )),
        }
    }
}

pub fn check_for_update_async() -> Receiver<CheckResult> {
    let (sender, receiver) = mpsc::channel();
    thread::Builder::new()
        .name("dee-bugee-update-check".to_string())
        .spawn(move || {
            let _ = sender.send(check_for_update().unwrap_or_else(CheckResult::Failed));
        })
        .expect("failed to start update-check worker");
    receiver
}

fn check_for_update() -> Result<CheckResult, String> {
    let release = latest_release()?;
    let available = parse_version(&release.tag_name)?;
    let current = parse_version(env!("CARGO_PKG_VERSION"))?;
    if available > current {
        Ok(CheckResult::Available(AvailableUpdate {
            version: available,
            tag_name: release.tag_name,
        }))
    } else {
        Ok(CheckResult::Current)
    }
}

pub fn start_update(restart_arguments: &[OsString]) -> Result<(), String> {
    let viewer_path =
        std::env::current_exe().map_err(|error| format!("Unable to locate DeeBugee: {error}"))?;
    let install_directory = viewer_path
        .parent()
        .ok_or_else(|| "DeeBugee has no install directory".to_string())?;
    let updater_target = install_directory.join(UPDATER_ASSET);
    if !updater_target.is_file() {
        return Err(
            "The DeeBugee updater component is missing. Run install.ps1 once to add it."
                .to_string(),
        );
    }

    let staged_updater = std::env::temp_dir().join(format!(
        "dee-bugee-updater-{}-{}.exe",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    fs::copy(&updater_target, &staged_updater)
        .map_err(|error| format!("Unable to stage the updater: {error}"))?;

    let mut command = Command::new(&staged_updater);
    command
        .arg("--wait-pid")
        .arg(std::process::id().to_string())
        .arg("--target")
        .arg(viewer_path)
        .arg("--updater-target")
        .arg(updater_target);
    for argument in restart_arguments {
        command.arg("--restart-arg").arg(argument);
    }
    command
        .spawn()
        .map_err(|error| format!("Unable to start the updater: {error}"))?;
    Ok(())
}

pub fn run_updater(arguments: Vec<OsString>) -> Result<(), String> {
    let request = UpdateRequest::parse(arguments)?;
    wait_for_process_exit(request.wait_pid)?;
    let release = latest_release()?;
    let viewer = release.asset(VIEWER_ASSET)?;
    let updater = release.asset(UPDATER_ASSET)?;
    replace_asset(viewer, &request.target)?;
    replace_asset(updater, &request.updater_target)?;

    Command::new(&request.target)
        .args(&request.restart_arguments)
        .spawn()
        .map_err(|error| format!("Unable to restart DeeBugee: {error}"))?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct UpdateRequest {
    wait_pid: u32,
    target: PathBuf,
    updater_target: PathBuf,
    restart_arguments: Vec<OsString>,
}

impl UpdateRequest {
    fn parse(arguments: Vec<OsString>) -> Result<Self, String> {
        let mut wait_pid = None;
        let mut target = None;
        let mut updater_target = None;
        let mut restart_arguments = Vec::new();
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.to_string_lossy().as_ref() {
                "--wait-pid" => {
                    let value = next_value(&mut arguments, "--wait-pid")?;
                    wait_pid = Some(
                        value
                            .to_string_lossy()
                            .parse()
                            .map_err(|_| "--wait-pid must be a process id".to_string())?,
                    );
                }
                "--target" => target = Some(PathBuf::from(next_value(&mut arguments, "--target")?)),
                "--updater-target" => {
                    updater_target = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--updater-target",
                    )?))
                }
                "--restart-arg" => {
                    restart_arguments.push(next_value(&mut arguments, "--restart-arg")?)
                }
                unknown => return Err(format!("Unknown updater option: {unknown}")),
            }
        }
        Ok(Self {
            wait_pid: wait_pid.ok_or_else(|| "--wait-pid is required".to_string())?,
            target: target.ok_or_else(|| "--target is required".to_string())?,
            updater_target: updater_target
                .ok_or_else(|| "--updater-target is required".to_string())?,
            restart_arguments,
        })
    }
}

fn next_value(
    arguments: &mut impl Iterator<Item = OsString>,
    option: &str,
) -> Result<OsString, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn latest_release() -> Result<Release, String> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| format!("Unable to create update client: {error}"))?
        .get(format!(
            "https://api.github.com/repos/{REPOSITORY}/releases/latest"
        ))
        .header(ACCEPT, "application/vnd.github+json")
        .header(USER_AGENT, "DeeBugee-Updater")
        .header("X-GitHub-Api-Version", API_VERSION)
        .send()
        .map_err(|error| format!("Unable to check GitHub releases: {error}"))?
        .error_for_status()
        .map_err(|error| format!("GitHub release check failed: {error}"))?
        .json()
        .map_err(|error| format!("GitHub returned invalid release metadata: {error}"))
}

fn replace_asset(asset: &ReleaseAsset, destination: &Path) -> Result<(), String> {
    let expected_hash = asset
        .digest
        .as_deref()
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| format!("Release asset {} has no valid SHA-256 digest", asset.name))?;
    let staged = destination.with_extension(format!("download.{}", uuid::Uuid::new_v4().simple()));
    let backup = destination.with_extension(format!("backup.{}", uuid::Uuid::new_v4().simple()));
    download_asset(&asset.browser_download_url, &staged)?;
    let actual_hash = sha256_file(&staged)?;
    if !actual_hash.eq_ignore_ascii_case(expected_hash) {
        let _ = fs::remove_file(&staged);
        return Err(format!("SHA-256 verification failed for {}", asset.name));
    }
    if destination.exists() {
        fs::rename(destination, &backup)
            .map_err(|error| format!("Unable to back up {}: {error}", destination.display()))?;
    }
    if let Err(error) = fs::rename(&staged, destination) {
        if backup.exists() {
            let _ = fs::rename(&backup, destination);
        }
        return Err(format!("Unable to install {}: {error}", asset.name));
    }
    let _ = fs::remove_file(backup);
    Ok(())
}

fn download_asset(url: &str, destination: &Path) -> Result<(), String> {
    let mut response = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|error| format!("Unable to create download client: {error}"))?
        .get(url)
        .header(USER_AGENT, "DeeBugee-Updater")
        .send()
        .map_err(|error| format!("Unable to download update: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Update download failed: {error}"))?;
    let mut file = fs::File::create(destination)
        .map_err(|error| format!("Unable to stage update: {error}"))?;
    std::io::copy(&mut response, &mut file)
        .map_err(|error| format!("Unable to save update: {error}"))?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut reader = BufReader::new(
        fs::File::open(path)
            .map_err(|error| format!("Unable to read downloaded update: {error}"))?,
    );
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 32 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|error| format!("Unable to verify update: {error}"))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn wait_for_process_exit(process_id: u32) -> Result<(), String> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::Threading::{INFINITE, OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject},
        };
        let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, process_id) };
        if handle.is_null() {
            return Err(format!("Unable to wait for DeeBugee process {process_id}"));
        }
        unsafe {
            WaitForSingleObject(handle, INFINITE);
            CloseHandle(handle);
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = process_id;
        Err("Auto-update is supported only on Windows".to_string())
    }
}

fn parse_version(value: &str) -> Result<Version, String> {
    Version::parse(value.trim_start_matches('v'))
        .map_err(|error| format!("Invalid release version {value:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_asset_requires_exactly_one_match() {
        let release = Release {
            tag_name: "v1.2.3".to_string(),
            assets: vec![ReleaseAsset {
                name: VIEWER_ASSET.to_string(),
                browser_download_url: "https://example.test/viewer".to_string(),
                digest: None,
            }],
        };
        assert_eq!(release.asset(VIEWER_ASSET).unwrap().name, VIEWER_ASSET);
        assert!(release.asset(UPDATER_ASSET).is_err());
    }

    #[test]
    fn version_tags_compare_as_semver() {
        assert!(parse_version("v1.10.0").unwrap() > parse_version("1.9.0").unwrap());
    }

    #[test]
    fn update_request_preserves_restart_arguments() {
        let request = UpdateRequest::parse(vec![
            "--wait-pid".into(),
            "42".into(),
            "--target".into(),
            "C:/DeeBugee/dee-bugee.exe".into(),
            "--updater-target".into(),
            "C:/DeeBugee/dee-bugee-updater.exe".into(),
            "--restart-arg".into(),
            "--project".into(),
            "--restart-arg".into(),
            "C:/work".into(),
        ])
        .unwrap();
        assert_eq!(request.wait_pid, 42);
        assert_eq!(
            request.restart_arguments,
            vec![OsString::from("--project"), OsString::from("C:/work")]
        );
    }
}
