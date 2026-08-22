fn main() {
    #[cfg(target_os = "windows")]
    {
        let mut resource = winres::WindowsResource::new();
        resource.set_icon("../../assets/deebugee-logo.ico");
        resource.set("ProductName", "DeeBugee");
        resource.set("FileDescription", "DeeBugee structured log viewer");
        resource
            .compile()
            .expect("DeeBugee Windows resources must compile");
    }
}
