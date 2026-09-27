fn main() {
    println!("cargo:rerun-if-changed=src/logql/grammar.lalrpop");
    lalrpop::process_root().expect("failed to generate LogQL token parser");
}
