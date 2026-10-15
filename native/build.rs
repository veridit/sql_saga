// Embed sql_saga's extension version (default_version in ../sql_saga.control) into the
// native library, so the library and the SQL that binds to it share one version.
fn main() {
    let control = "../sql_saga.control";
    println!("cargo:rerun-if-changed={control}");
    let text = std::fs::read_to_string(control).expect("read ../sql_saga.control");
    let version = text
        .lines()
        .find_map(|l| {
            let l = l.trim();
            let rest = l.strip_prefix("default_version")?.trim_start().strip_prefix('=')?;
            Some(rest.trim().trim_matches('\'').to_string())
        })
        .expect("default_version not found in ../sql_saga.control");
    println!("cargo:rustc-env=SQL_SAGA_EXTVERSION={version}");
}
