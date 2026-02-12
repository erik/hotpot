use std::process::Command;

fn git_hash() -> Option<String> {
    std::env::var("GIT_HASH").ok().or_else(|| {
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
            .ok()
    })
}

fn main() {
    let mut version = env!("CARGO_PKG_VERSION").to_string();

    if let Some(hash) = git_hash()
        && !hash.is_empty()
    {
        version = format!("{} ({})", version, &hash[..7]);
    }

    println!("cargo:rustc-env=VERSION_STRING={}", version);
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=.git/HEAD");
}
