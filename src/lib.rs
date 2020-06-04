use std::error::Error;
use std::fs::{File, FileType};
use std::io::Write;
use std::path::Path;

use chrono::{Date, NaiveDate, NaiveDateTime, NaiveTime};
use odbc::{create_environment_v3, SqlDate, SqlTime, SqlTimestamp};
use odbc::odbc_safe::AutocommitOn;
use odbc::ResultSetState::{Data, NoData};
use odbc::{Connection, Statement};

use crate::column_type::ColumnType;
use crate::encoding::encode_value;
use crate::sql_data_type::SqlDataType;

mod column_type;
mod encoding;
mod sql_data_type;

const GET_COLUMN_DEFINITIONS_QUERY: &str = include_str!("sql/get_column_definitions.sql");
const SELECT_ALL_QUERY: &str = include_str!("sql/select_all.sql");
const FILE_HEADER: [u8; 11] = [
    0x4E, 0x41, 0x54, 0x49, 0x56, 0x45, 0x0A, 0xFF, 0x0D, 0x0A, 0x00,
];

pub fn extract(
    server: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    table: String,
    limit: Option<usize>,
    output_path: &Path,
) -> std::result::Result<(), Box<dyn Error>> {
    let dsn = create_dsn(server, port, database, username, password).clone();

    let env = create_environment_v3().map_err(|e| e.unwrap())?;
    let conn = env.connect_with_connection_string(&dsn)?;

    let column_types: Vec<ColumnType> = get_column_types(&conn, &table)?;

    let column_definitions = generate_column_definitions(&column_types);

    let limit = match limit {
        None => "".to_string(),
        Some(limit) => String::from(format!("limit {}", limit)),
    };

    let query = SELECT_ALL_QUERY
        .replace("XX_TABLE_NAME_XX", table.as_str())
        .replace("XX_LIMIT_XX", limit.as_str());

    let stmt = Statement::with_parent(&conn)?;

    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            let mut output_file = File::create(&output_path)?;
            output_file.write(&FILE_HEADER);
            output_file.write(column_definitions.as_slice());

            let cols = stmt.num_result_cols()?;

            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    let col_type = &column_types[(i - 1) as usize];

                    let byte_val: Vec<u8> = match col_type.data_type {
                        SqlDataType::Integer | SqlDataType::Interval => {
                            let value = cursor.get_data::<i64>(i as u16)?;
                            encode_value::<Option<i64>>(value, col_type)
                        }
                        SqlDataType::Float => {
                            let value = cursor.get_data::<f64>(i as u16)?;
                            encode_value::<Option<f64>>(value, col_type)
                        }
                        SqlDataType::Char | SqlDataType::Varchar => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            encode_value::<Option<&str>>(value, col_type)
                        }
                        SqlDataType::Boolean => match cursor.get_data::<bool>(i as u16)? {
                            None => vec![0],
                            Some(b) => vec![b as u8],
                        },
                        SqlDataType::Date => {
                            let value = cursor.get_data::<SqlDate>(i as u16)?;
                            encode_value::<Option<SqlDate>>(value, col_type)
                        }
                        SqlDataType::Timestamp => {
                            let value = cursor.get_data::<SqlTimestamp>(i as u16)?;
                            encode_value::<Option<SqlTimestamp>>(value, col_type)
                        }
                        SqlDataType::TimestampTz => {
                            let value = cursor.get_data::<SqlTimestamp>(i as u16)?;
                            encode_value::<Option<SqlTimestamp>>(value, col_type)
                        }
                        SqlDataType::Time => {
                            let value = cursor.get_data::<SqlTime>(i as u16)?;
                            encode_value::<Option<SqlTime>>(value, col_type)
                        }
                        SqlDataType::TimeTz => {
                            let value = cursor.get_data::<SqlTime>(i as u16)?;
                            encode_value::<Option<SqlTime>>(value, col_type)
                        }
                        SqlDataType::Varbinary | SqlDataType::Binary | SqlDataType::Numeric => {
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            encode_value::<Option<Vec<u8>>>(value, col_type)
                        }
                    };

                    // let bytes = encoding::encode_value(value, &col_type);
                }
            }
        }
        NoData(_) => println!("no data returned"),
    };

    Ok(())
}

fn generate_column_definitions(column_types: &Vec<ColumnType>) -> Vec<u8> {
    let mut bytes: Vec<u8> = vec![];
    let mut sizes: Vec<u32> = vec![];

    // file version; only supported version is `1`
    bytes.extend_from_slice(&1_u16.to_le_bytes()[..]);

    // single-byte filler; value `0`
    bytes.push(0);

    // number of columns
    bytes.extend_from_slice(&(column_types.len() as u16).to_le_bytes()[..]);

    for column_type in column_types {
        let width: u32 = match column_type.data_type {
            SqlDataType::Integer | SqlDataType::Char | SqlDataType::Binary => {
                column_type.width as u32
            }
            SqlDataType::Varchar | SqlDataType::Varbinary => -1_i32 as u32,
            SqlDataType::Boolean => 1,
            SqlDataType::Float
            | SqlDataType::Date
            | SqlDataType::Timestamp
            | SqlDataType::TimestampTz
            | SqlDataType::Time
            | SqlDataType::TimeTz
            | SqlDataType::Interval => 8,
            SqlDataType::Numeric => {
                if let Some(precision) = column_type.precision {
                    (((precision / 19) + 1) * 8) as u32
                } else {
                    0
                }
            }
        };

        sizes.push(width);
        bytes.extend_from_slice(&width.to_le_bytes()[..]);
    }

    let header_length = bytes.len() as u32;

    let mut header: Vec<u8> = vec![];

    header.extend_from_slice(&header_length.to_le_bytes()[..]);

    for byte in bytes {
        header.push(byte);
    }

    header
}

fn get_column_types<'env>(
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
