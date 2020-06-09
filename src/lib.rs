use std::convert::TryInto;
use std::error::Error;
use std::fs::{File, FileType};
use std::io::Write;
use std::mem::size_of;
use std::ops::Sub;
use std::path::Path;

use chrono::{Date, NaiveDate, NaiveDateTime, NaiveTime};
use odbc::odbc_safe::sys::SQL_DATE_STRUCT;
use odbc::odbc_safe::AutocommitOn;
use odbc::ResultSetState::{Data, NoData};
use odbc::{create_environment_v3, SqlDate, SqlTime, SqlTimestamp};
use odbc::{Connection, Statement};

use crate::column_type::ColumnType;
use crate::sql_data_type::SqlDataType;

mod column_type;
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
        NoData(_) => println!("no data returned"),
        Data(mut stmt) => {
            let mut output_file = File::create(&output_path)?;
            output_file.write(&FILE_HEADER);
            output_file.write(column_definitions.as_slice());

            let cols = stmt.num_result_cols()?;
            let mut nulls: Vec<bool> = vec![false; cols as usize];
            let mut values: Vec<Vec<u8>> = vec![];

            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    let col_type = &column_types[(i - 1) as usize];

                    let byte_val: Vec<u8> = match col_type.data_type {
                        SqlDataType::Integer => {
                            let value = cursor.get_data::<i64>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let x = value.to_le_bytes();
                                    println!("{:?}", x);
                                    let vx = x.to_vec();
                                    println!("{:?}", vx);

                                    vx
                                },
                            }
                        }
                        SqlDataType::Interval => {
                            let value = cursor.get_data::<&[u8]>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.to_vec(),
                            }
                        }
                        SqlDataType::Float => {
                            let value = cursor.get_data::<f64>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.to_le_bytes().to_vec(),
                            }
                        }
                        SqlDataType::Char => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.as_bytes().to_vec(),
                            }
                        }
                        SqlDataType::Varchar => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let bytes = value.as_bytes();
                                    let byte_len: u32 = bytes.len() as u32;

                                    let mut rec: Vec<u8> = byte_len.to_le_bytes().to_vec();
                                    rec.extend_from_slice(bytes);

                                    rec
                                }
                            }
                        }
                        SqlDataType::Boolean => match cursor.get_data::<bool>(i as u16)? {
                            None => {
                                nulls[i as usize] = true;
                                vec![]
                            }
                            Some(b) => vec![b as u8],
                        },
                        SqlDataType::Date => {
                            let value = cursor.get_data::<SqlDate>(i as u16)?;

                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let epoch = NaiveDate::from_ymd(2000, 1, 1);
                                    let the_date = NaiveDate::from_ymd(
                                        value.year as i32,
                                        value.month as u32,
                                        value.day as u32,
                                    );

                                    let diff = (the_date - epoch).num_days();

                                    diff.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::Timestamp => {
                            let value = cursor.get_data::<SqlTimestamp>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let epoch =
                                        NaiveDate::from_ymd(2000, 1, 1).and_hms_milli(0, 0, 0, 0);
                                    let the_date = NaiveDate::from_ymd(
                                        value.year as i32,
                                        value.month as u32,
                                        value.day as u32,
                                    )
                                    .and_hms_nano(
                                        value.hour as u32,
                                        value.minute as u32,
                                        value.second as u32,
                                        value.fraction as u32,
                                    );

                                    let diff = match (the_date - epoch).num_microseconds() {
                                        None => 0,
                                        Some(diff) => diff,
                                    };

                                    diff.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::TimestampTz => {
                            // TODO: either this one or Timestamp needs to be adjusted for local TZ
                            let value = cursor.get_data::<SqlTimestamp>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let epoch =
                                        NaiveDate::from_ymd(2000, 1, 1).and_hms_milli(0, 0, 0, 0);
                                    let the_date = NaiveDate::from_ymd(
                                        value.year as i32,
                                        value.month as u32,
                                        value.day as u32,
                                    )
                                    .and_hms_nano(
                                        value.hour as u32,
                                        value.minute as u32,
                                        value.second as u32,
                                        value.fraction as u32,
                                    );

                                    let diff = match (the_date - epoch).num_microseconds() {
                                        None => 0,
                                        Some(diff) => diff,
                                    };

                                    diff.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::Time => {
                            let value = cursor.get_data::<SqlTime>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);

                                    let the_time = NaiveTime::from_hms(
                                        value.hour as u32,
                                        value.minute as u32,
                                        value.second as u32,
                                    );

                                    let diff = match (the_time - midnight).num_microseconds() {
                                        None => 0,
                                        Some(diff) => diff,
                                    };

                                    diff.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::TimeTz => {
                            let value = cursor.get_data::<SqlTime>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);

                                    let the_time = NaiveTime::from_hms(
                                        value.hour as u32,
                                        value.minute as u32,
                                        value.second as u32,
                                    );

                                    let diff = match (the_time - midnight).num_microseconds() {
                                        None => 0,
                                        Some(diff) => diff,
                                    };

                                    diff.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::Varbinary | SqlDataType::Binary => {
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            match value {
                                None => {
                                    nulls[i as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let byte_len: u32 = value.len() as u32;

                                    let mut rec: Vec<u8> = byte_len.to_le_bytes().to_vec();
                                    rec.extend(value);

                                    rec
                                }
                            }
                        }
                        SqlDataType::Numeric => {
                            nulls[i as usize] = true;
                            vec![]
                        }
                    };

                    &values.push(byte_val);
                }

                let bitmap = create_nulls_bitmap(cols, &nulls);

                let row_size = bitmap.len() + (&values).iter().fold(0, |acc, x| acc + x.len());

                let flattened_values = (&values)
                    .into_iter()
                    .flatten()
                    .map(|v| *v)
                    .collect::<Vec<u8>>();

                println!("{:X?}", values);
                println!("{:X?}", flattened_values);

                output_file.write(&row_size.to_le_bytes());
                output_file.write(&bitmap.as_slice());
                // output_file.write(&flattened_values);
                output_file.write_all(&flattened_values);
            }
        }
    };

    Ok(())
}

fn create_nulls_bitmap(cols: i16, nulls: &Vec<bool>) -> Vec<u8> {
    let multiplier = size_of::<u8>() as i16 * 8;
    let bytes_needed = cols / multiplier + if cols % multiplier != 0 { 1 } else { 0 };

    let mut bitmap: Vec<u8> = vec![];

    for byte_index in 0..bytes_needed as usize {
        let mut byte: u8 = 0;
        for i in 0..8 {
            let bitfield_index = (i as i8 - 8).abs() - 1;
            let i_adjusted = i * byte_index;

            let null_or_not: u8 = if nulls[i_adjusted] { 1 } else { 0 };

            byte |= (null_or_not << bitfield_index as u8);
        }

        bitmap.insert(0, byte);
    }

    bitmap
}

fn generate_column_definitions(column_types: &Vec<ColumnType>) -> Vec<u8> {
    // file version; only supported version is `1`
    let mut bytes: Vec<u8> = 1_u16.to_le_bytes().to_vec();

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

        bytes.extend_from_slice(&width.to_le_bytes()[..]);
    }

    let header_length = bytes.len() as u32;

    let mut header: Vec<u8> = header_length.to_le_bytes().to_vec();
    header.extend(bytes);

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
