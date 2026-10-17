// Records the commit, clean/dirty state, and compiler in the binary (`--version`).
#[path = "build_info.rs"]
mod build_info;

fn main() {
    build_info::main();
}
