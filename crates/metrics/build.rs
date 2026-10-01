use std::path::Path;
use std::{env, fs};

fn main() {
    let testdata_dir = Path::new("src/promql/promqltest/testdata");
    println!("cargo:rerun-if-changed={}", testdata_dir.display());

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR must be set");
    let dest = Path::new(&out_dir).join("promql_tests_generated.rs");
    let mut code = String::new();

    if testdata_dir.exists() {
        let mut entries: Vec<_> = fs::read_dir(testdata_dir)
            .expect("failed to read PromQL testdata")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("test"))
            .collect();
        entries.sort_by_key(std::fs::DirEntry::file_name);

        for entry in entries {
            let path = entry.path();
            let stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("invalid test filename");
            let function = stem.replace('-', "_");
            code.push_str(&format!(
                "\n#[tokio::test(flavor = \"multi_thread\", worker_threads = 2)]\n\
                 async fn should_pass_{function}() {{\n\
                 run_test(\"{stem}\", include_str!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \
                 \"/src/promql/promqltest/testdata/{stem}.test\"))).await.unwrap();\n}}\n"
            ));
        }
    }

    fs::write(dest, code).expect("failed to write generated PromQL tests");
}
