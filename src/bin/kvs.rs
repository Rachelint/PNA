extern crate exitcode;
use clap::{Parser, Subcommand};


/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author="ray", version=env!("CARGO_PKG_VERSION"), about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[clap(arg_required_else_help=true)]
    Get {
        /// The remote to clone
        key: String,
    },

    #[clap(arg_required_else_help=true)]
    Set {
        /// The remote to clone
        key: String,
        value: String,
    },

    #[clap(arg_required_else_help=true)]
    Rm {
        /// The remote to clone
        key: String,
    },
}

fn main() {
    let args = Args::parse();
    match args.command {
        Commands::Get{key} => {
            eprintln!("get key:{}, unimplemented", key);
            std::process::exit(exitcode::SOFTWARE);
        },

        Commands::Set{key, value} => {
            eprintln!("set key:{} value:{}, unimplemented", key, value);
            std::process::exit(exitcode::SOFTWARE);
        },

        Commands::Rm{key} => {
            eprintln!("rm key:{}, unimplemented", key);
            std::process::exit(exitcode::SOFTWARE);
        },
    }
}