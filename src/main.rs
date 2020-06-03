use std::env;
use std::path::Path;

use getopts::Options;

use verticaextractor::extract;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "s",
        "server",
        "server to connect to [default: localhost]",
        "NAME",
    );
    opts.optopt("p", "port", "port to connect to [default: 5433]", "NUMBER");
    opts.optopt("d", "database", "database to extract from", "NAME");
    opts.optopt(
        "u",
        "username",
        "username for login [default: dbadmin]",
        "NAME",
    );
    opts.optopt(
        "P",
        "password",
        "password for user [default: none]",
        "PASSWORD",
    );
    opts.optopt("o", "output", "output file name", "NAME");
    opts.optopt("t", "table", "table to extract", "NAME");
    opts.optopt(
        "l",
        "limit",
        "maximum number of rows to extract from <table>",
        "NUMBER",
    );
    opts.optflag("h", "help", "display this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            eprintln!("unable to parse options: {}", f.to_string());
            print_usage(&program, opts);
            return;
        }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let server = match matches.opt_get_default("s", "localhost".to_string()) {
        Ok(server) => server,
        Err(_) => {
            eprintln!("server name must be given as a string");
            print_usage(&program, opts);
            return;
        }
    };

    let port = match matches.opt_get_default("p", 5433) {
        Ok(port) => port,
        Err(_) => {
            eprintln!("port must be given as an integer");
            print_usage(&program, opts);
            return;
        }
    };

    let database = match matches.opt_str("d") {
        None => {
            eprintln!("database is required");
            print_usage(&program, opts);
            return;
        }
        Some(database) => database,
    };

    let username = match matches.opt_get_default("u", "dbadmin".to_string()) {
        Ok(username) => username,
        Err(_) => {
            eprintln!("username must be given as a string");
            print_usage(&program, opts);
            return;
        }
    };

    let password = matches.opt_str("P");

    let output = match matches.opt_str("o") {
        None => {
            eprintln!("output file name is required");
            print_usage(&program, opts);
            return;
        }
        Some(o) => o,
    };

    let table = match matches.opt_str("t") {
        None => {
            eprintln!("table name is required");
            print_usage(&program, opts);
            return;
        }
        Some(table) => table,
    };

    let limit = match matches.opt_get::<usize>("l") {
        Ok(limit) => limit,
        Err(_) => {
            eprintln!("limit must be given as an integer");
            print_usage(&program, opts);
            return;
        }
    };

    let output_path = Path::new(&output);

    // if output_path.exists() {
    //     eprintln!("file [{}] exists", output);
    //     return;
    // }

    match extract(
        server,
        port,
        database,
        username,
        password,
        table,
        limit,
        output_path,
    ) {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);

    println!("{}", opts.usage(&brief));
}
