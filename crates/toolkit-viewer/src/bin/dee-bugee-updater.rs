#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

#[allow(dead_code)]
#[path = "../update.rs"]
mod update;

fn main() {
    if let Err(error) = update::run_updater(std::env::args_os().skip(1).collect()) {
        #[cfg(windows)]
        rfd::MessageDialog::new()
            .set_level(rfd::MessageLevel::Error)
            .set_title("DeeBugee update failed")
            .set_description(&error)
            .set_buttons(rfd::MessageButtons::Ok)
            .show();
        eprintln!("DeeBugee update failed: {error}");
        std::process::exit(1);
    }
}
