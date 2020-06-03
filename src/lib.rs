use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use odbc::create_environment_v3;
use odbc::odbc_safe::sys::SqlDataType;
use odbc::odbc_safe::AutocommitOn;
use odbc::ResultSetState::{Data, NoData};
use odbc::{Connection, Statement};

use crate::column_type::ColumnType;

mod column_type;
mod sql_data_type;

const GET_COLUMN_DEFINITIONS_QUERY: &str = include_str!("sql/get_column_definitions.sql");
const SELECT_ALL_QUERY: &str = include_str!("sql/select_all.sql");

pub fn extract(
    server: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    table: String,
    _limit: Option<usize>,
    output_path: &Path,
) -> std::result::Result<(), Box<dyn Error>> {
    let dsn = create_dsn(server, port, database, username, password).clone();

    let env = create_environment_v3().map_err(|e| e.unwrap())?;
    let conn = env.connect_with_connection_string(&dsn)?;

    let column_types: Vec<ColumnType> = get_column_definitions(&conn, &table)?;

    let query = SELECT_ALL_QUERY.replace("XX_TABLE_NAME_XX", table.as_str());

    let stmt = Statement::with_parent(&conn)?;

    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            let mut output_file = File::create(&output_path)?;

            let cols = stmt.num_result_cols()?;

            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    let col_type = &column_types[(i - 1) as usize];
                    println!("CT: {:?}", &col_type);
                }
            }
        }
        NoData(_) => println!("no data returned"),
    };

    Ok(())
}

fn get_column_definitions<'env>(
    conn: &Connection<'env, AutocommitOn>,
    table: &String,
) -> std::result::Result<Vec<ColumnType>, Box<dyn Error>> {
    let query = GET_COLUMN_DEFINITIONS_QUERY.replace("XX_TABLE_NAME_XX", table.as_str());
    let stmt = Statement::with_parent(&conn)?;

    let mut column_types: Vec<ColumnType> = vec![];

    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;

            while let Some(mut cursor) = stmt.fetch()? {
                let mut values: Vec<String> = vec![];

                for i in 1..(cols + 1) {
                    match cursor.get_data::<&str>(i as u16)? {
                        Some(val) => values.push(val.to_string()),
                        None => values.push("".to_string()),
                    }
                }

                column_types.push(ColumnType::new(&values));
            }
        }
        NoData(_) => println!("no data returned"),
    };

    Ok(column_types)
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
