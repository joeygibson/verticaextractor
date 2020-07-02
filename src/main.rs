use std::env;
use std::path::Path;

use colored::*;
use getopts::Options;

use verticaextractor::extract;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();

    let database_help = format!("database to extract from {}", "*".bright_red());
    opts.optopt("d", "database", database_help.as_str(), "NAME");

    let table_help = format!("table to extract {}", "*".bright_red());
    opts.optopt("t", "table", table_help.as_str(), "NAME");

    let output_help = format!("output file name {}", "*".bright_red());
    opts.optopt("o", "output", output_help.as_str(), "NAME");

    let server_help = format!(
        "server to connect to {}",
        "[default: localhost]".bright_green()
    );
    opts.optopt("s", "server", server_help.as_str(), "NAME");

    let port_help = format!("port to connect to {}", "[default: 5433]".bright_green());
    opts.optopt("p", "port", port_help.as_str(), "NUMBER");

    let username_help = format!("username for login {}", "[default: dbadmin]".bright_green());
    opts.optopt("u", "username", username_help.as_str(), "NAME");
    opts.optopt("P", "password", "password for user", "PASSWORD");

    opts.optflag("f", "force", "overwrite destination file");

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
            let msg = format!("\nerror unable to parse options: {}\n", f.to_string());
            eprintln!("{}", msg.bright_red());
            print_usage(&program, opts);
            return;
        }
    };

    // If no options at all, or `-h` are passed, print out the help,
    // without an error, and exit
    if matches.opt_present("h")
        || !matches.opts_present(&[
            "table".to_string(),
            "output".to_string(),
            "database".to_string(),
        ])
    {
        print_usage(&program, opts);
        return;
    }

    let server = match matches.opt_get_default("s", "localhost".to_string()) {
        Ok(server) => server,
        Err(_) => {
            eprintln!(
                "{}",
                "\nerror: server name must be given as a string\n".bright_red()
            );
            print_usage(&program, opts);
            return;
        }
    };

    let port = match matches.opt_get_default("p", 5433) {
        Ok(port) => port,
        Err(_) => {
            eprintln!(
                "{}",
                "\nerror: port must be given as an integer\n".bright_red()
            );
            print_usage(&program, opts);
            return;
        }
    };

    let database = match matches.opt_str("d") {
        None => {
            eprintln!("{}", "\nerror: database is required\n".bright_red());
            print_usage(&program, opts);
            return;
        }
        Some(database) => database,
    };

    let username = match matches.opt_get_default("u", "dbadmin".to_string()) {
        Ok(username) => username,
        Err(_) => {
            eprintln!(
                "{}",
                "\nerror: username must be given as a string\n".bright_red()
            );
            print_usage(&program, opts);
            return;
        }
    };

    let output = match matches.opt_str("o") {
        None => {
            eprintln!("{}", "\nerror: output file name is required\n".bright_red());
            print_usage(&program, opts);
            return;
        }
        Some(o) => o,
    };

    let table = match matches.opt_str("t") {
        None => {
            eprintln!("{}", "\nerror: table name is required\n".bright_red());
            print_usage(&program, opts);
            return;
        }
        Some(table) => table,
    };

    let limit = match matches.opt_get::<usize>("l") {
        Ok(limit) => limit,
        Err(_) => {
            eprintln!(
                "{}",
                "\nerror: limit must be given as an integer\n".bright_red()
            );
            print_usage(&program, opts);
            return;
        }
    };

    let output_path = Path::new(&output);

    if output_path.exists() && !matches.opt_present("f") {
        let msg = format!("\nerror: file [{}] exists; use `-f` to force\n", output);
        eprintln!("{}", msg.bright_red());
        return;
    }

    let password = match matches.opt_str("P") {
        None => get_password_from_user(),
        Some(password) => Some(password),
    };

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
        Err(e) => {
            let msg = format!("Error: {}", e);
            eprintln!("{}", msg.bright_red())
        }
    }
}

fn get_password_from_user() -> Option<String> {
    match rpassword::prompt_password_stdout("Password: ") {
        Ok(password) => Some(password),
        Err(e) => {
            eprintln!("getting password: {}", e);
            None
        }
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!(
        "Usage: {} [options]\n\toptions with * are required",
        program
    );

    println!("{}", opts.usage(&brief));
}
