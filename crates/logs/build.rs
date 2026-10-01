fn main() {
    println!("cargo:rerun-if-changed=src/logql/grammar.lalrpop");
    println!("cargo:rerun-if-changed=src/query/template/go/grammar.lalrpop");
    lalrpop::process_root().expect("failed to generate LALRPOP parsers");
}
