fn main() {
    println!("cargo:rerun-if-changed=src/traceql/grammar.lalrpop");
    lalrpop::process_root().expect("failed to generate TraceQL parser");
}
