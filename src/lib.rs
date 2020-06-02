use std::error::Error;

use odbc::create_environment_v3;
use odbc::ResultSetState::{Data, NoData};
use odbc::Statement;

const SELECT_ALL_QUERY: &str = include_str!("sql/select_all.sql");

pub fn extract(
    server: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    table: String,
    limit: Option<usize>,
    output: String,
) -> std::result::Result<(), Box<dyn Error>> {
    let dsn = create_dsn(server, port, database, username, password).clone();

    let env = create_environment_v3().map_err(|e| e.unwrap())?;
    let conn = env.connect_with_connection_string(&dsn)?;

    let stmt = Statement::with_parent(&conn)?;

    let query = SELECT_ALL_QUERY.replace("XX_TABLE_NAME_XX", table.as_str());

    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;

            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    let c = match stmt.describe_col(i as u16) {
                        Ok(c) => c,
                        Err(_) => panic!("unable to describe column: {}", i),
                    };

                    println!("{:?}", c);
                }

                // for i in 1..(cols + 1) {
                //     match cursor.get_data::<&str>(i as u16)? {
                //         Some(val) => print!(" {}", val),
                //         None => print!(" NULL"),
                //     }
                // }

                println!();
            }
        }
        NoData(_) => println!("no data returned"),
    }

    Ok(())
}

fn create_dsn(
    server: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
) -> String {
    format!(
        "Driver=Vertica;ServerName={};Port={};Database={};UID={}{}",
        server,
        port,
        database,
        username,
        match password {
            None => "".to_string(),
            Some(password) => format!(";PWD={}", password),
        }
    )
}
