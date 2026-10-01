use std::{env, fs, path::Path};

use anyhow::{Context, Result, bail};

fn main() -> Result<()> {
    let check = match env::args().nth(1).as_deref() {
        None => false,
        Some("--check") => true,
        Some(argument) => bail!("unknown argument {argument}; expected --check"),
    };
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("api-docs must live under <repository>/crates")?;
    let documents = [
        (
            "Metrics",
            repository.join("documentation/openapi/metrics.json"),
            plural_metrics_server::openapi::document().to_pretty_json()?,
        ),
        (
            "Logs",
            repository.join("documentation/openapi/logs.json"),
            plural_logs_server::openapi::document().to_pretty_json()?,
        ),
        (
            "Traces",
            repository.join("documentation/openapi/traces.json"),
            plural_traces_server::openapi::document().to_pretty_json()?,
        ),
    ];

    let mut stale = Vec::new();
    for (name, path, document) in documents {
        let document = format!("{document}\n");
        if check {
            match fs::read_to_string(&path) {
                Ok(current) if current == document => {}
                Ok(_) | Err(_) => stale.push(path),
            }
        } else {
            fs::create_dir_all(
                path.parent()
                    .context("OpenAPI document path must have a parent")?,
            )
            .with_context(|| format!("failed to create directory for {name} OpenAPI document"))?;
            fs::write(&path, document)
                .with_context(|| format!("failed to write {} OpenAPI document", name))?;
            println!("generated {}", path.display());
        }
    }

    if stale.is_empty() {
        Ok(())
    } else {
        let paths = stale
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("OpenAPI documents are stale or missing: {paths}")
    }
}
