//! Embed the active C++ runtime directory as an RUNPATH in analytics binaries.
//!
//! The bundled `duckdb` feature compiles a C++ static library, so every binary
//! and test binary in this package depends dynamically on `libstdc++.so.6`.
//! On hosts whose C++ runtime lives outside the default loader search path
//! (NixOS), the loader cannot find it: linking succeeds but running the test
//! binary fails with exit 127 (`error while loading shared libraries:
//! libstdc++.so.6`). `scripts/definition-of-done.sh` works around this in
//! full mode by exporting `LD_LIBRARY_PATH`, but that workaround cannot reach
//! scrubbed verification environments (NEEDLE's close-verification gate strips
//! the environment to an allowlist, so `LD_LIBRARY_PATH` and
//! `XDG_RUNTIME_DIR` are absent there). Asking the active C++ compiler for its
//! runtime — the same lookup the definition-of-done script performs — and
//! recording it as an RUNPATH makes the loader resolution independent of the
//! environment. On hosts where the runtime is already on the default search
//! path, the recorded directory is simply searched (and found) first.

fn main() {
    let Ok(output) = std::process::Command::new("g++")
        .arg("-print-file-name=libstdc++.so.6")
        .output()
    else {
        // No C++ compiler on PATH: the default loader search paths apply.
        return;
    };
    if !output.status.success() {
        return;
    }
    let libstdcxx = String::from_utf8_lossy(&output.stdout).trim().to_string();
    // The compiler prints the bare file name when it has no matching file of
    // its own; only an absolute path identifies a usable RUNPATH directory.
    if !libstdcxx.starts_with('/') {
        return;
    }
    if let Some(dir) = std::path::Path::new(&libstdcxx).parent() {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
    }
}
