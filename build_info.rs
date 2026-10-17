use std::process::Command;

fn command_text(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn main() {
    println!("cargo:rerun-if-changed={}", file!());
    let sha = command_text("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let state = match command_text(
        "git",
        &["status", "--porcelain", "--untracked-files=normal"],
    ) {
        Some(status) if status.is_empty() => "clean",
        Some(_) => "dirty",
        None => "unknown",
    };
    for git_path in ["HEAD", "index"] {
        if let Some(path) = command_text("git", &["rev-parse", "--git-path", git_path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = command_text("git", &["symbolic-ref", "-q", "HEAD"])
        && let Some(path) = command_text("git", &["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={path}");
    }
    if let Some(root) = command_text("git", &["rev-parse", "--show-toplevel"])
        && let Some(files) = command_text(
            "git",
            &[
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "--full-name",
            ],
        )
    {
        for file in files.lines() {
            println!("cargo:rerun-if-changed={root}/{file}");
        }
    }
    let mut features: Vec<_> = std::env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
                .map(|feature| feature.to_ascii_lowercase().replace('_', "-"))
        })
        .collect();
    features.sort();
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let rustc = command_text(&rustc, &["--version"]).unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=AGENT_PHONE_BUILD_GIT_SHA={sha}");
    println!("cargo:rustc-env=AGENT_PHONE_BUILD_GIT_STATE={state}");
    println!(
        "cargo:rustc-env=AGENT_PHONE_BUILD_FEATURES={}",
        features.join(",")
    );
    println!("cargo:rustc-env=AGENT_PHONE_BUILD_RUSTC={rustc}");
}
