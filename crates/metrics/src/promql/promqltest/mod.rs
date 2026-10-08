mod assert;
mod dsl;
mod evaluator;
mod loader;
#[cfg(test)]
mod range_cache;
pub mod runner;

#[cfg(test)]
mod tests {
    use super::runner::run_test;

    include!(concat!(env!("OUT_DIR"), "/promql_tests_generated.rs"));
}
