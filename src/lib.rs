use std::convert::TryInto;
use std::error::Error;
use std::fs::{File, FileType};
use std::io::Write;
use std::mem::size_of;
use std::ops::Sub;
use std::path::Path;
use std::str::FromStr;

use chrono::{Date, DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use odbc::odbc_safe::sys::{SQLINTERVAL, SQL_DATE_STRUCT, SQL_INTERVAL_STRUCT, SQL_INTERVAL_UNION};
use odbc::odbc_safe::AutocommitOn;
use odbc::ResultSetState::{Data, NoData};
use odbc::{create_environment_v3, EncodedValue, OdbcType, SqlDate, SqlTime, SqlTimestamp};
use odbc::{Connection, Statement};
use regex::Regex;

use lazy_static::lazy_static;

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

            let mut rows_written: u32 = 0;

            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    let col_type = &column_types[(i - 1) as usize];

                    let byte_val: Vec<u8> = match col_type.data_type {
                        SqlDataType::Integer => {
                            let value = cursor.get_data::<i64>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.to_le_bytes().to_vec(),
                            }
                        }
                        SqlDataType::Interval => {
                            // TODO: Try SqlIntervalStruct
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    value
                                    // lazy_static! {
                                    //     static ref chunker: Regex =
                                    //         Regex::new(r"\d*\s*(\d+):(\d+):(\d+)\.(\d+)").unwrap();
                                    // }
                                    //
                                    // if chunker.is_match(value) {
                                    //     let captures = chunker.captures(value).unwrap();
                                    //     let hours: u32 = captures
                                    //         .get(1)
                                    //         .map_or("", |m| m.as_str())
                                    //         .parse()
                                    //         .unwrap();
                                    //     let minutes: u32 = captures
                                    //         .get(2)
                                    //         .map_or("", |m| m.as_str())
                                    //         .parse()
                                    //         .unwrap();
                                    //     let seconds: u32 = captures
                                    //         .get(3)
                                    //         .map_or("", |m| m.as_str())
                                    //         .parse()
                                    //         .unwrap();
                                    //     let millis: u32 = captures
                                    //         .get(4)
                                    //         .map_or("", |m| m.as_str())
                                    //         .parse()
                                    //         .unwrap();
                                    //
                                    //     let res = (hours * 3600000000)
                                    //         + (minutes * 60000000)
                                    //         + (seconds * 1000000)
                                    //         + (millis * 1000);
                                    //
                                    //     res.to_le_bytes().to_vec()
                                    // } else {
                                    //     vec![]
                                    // }
                                }
                            }
                        }
                        SqlDataType::Float => {
                            let value = cursor.get_data::<f64>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.to_le_bytes().to_vec(),
                            }
                        }
                        SqlDataType::Char => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => value.as_bytes().to_vec(),
                            }
                        }
                        SqlDataType::Varchar => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
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
                                nulls[(i - 1) as usize] = true;
                                vec![]
                            }
                            Some(b) => vec![b as u8],
                        },
                        SqlDataType::Date => {
                            let value = cursor.get_data::<SqlDate>(i as u16)?;

                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
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
                        SqlDataType::Timestamp | SqlDataType::TimestampTz => {
                            let value = cursor.get_data::<SqlTimestamp>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
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
                                    nulls[(i - 1) as usize] = true;
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
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);
                                    let local_now = Local::now();
                                    let local_local = local_now.naive_local();
                                    let local_utc = local_now.naive_utc();

                                    let hour = u16::from_le_bytes(value[0..2].try_into().unwrap());
                                    let minute =
                                        u16::from_le_bytes(value[2..4].try_into().unwrap());
                                    let second =
                                        u16::from_le_bytes(value[4..6].try_into().unwrap());

                                    let the_time = NaiveTime::from_hms(
                                        hour as u32,
                                        minute as u32,
                                        second as u32,
                                    );

                                    let diff = match (the_time - midnight).num_microseconds() {
                                        None => 0,
                                        Some(diff) => diff,
                                    };

                                    let tz_diff_seconds =
                                        (local_local - local_utc).num_seconds() + (24 * 60 * 60);

                                    let total = (diff << 24) + tz_diff_seconds;

                                    total.to_le_bytes().to_vec()
                                }
                            }
                        }
                        SqlDataType::Varbinary => {
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
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
                        SqlDataType::Binary => {
                            let value = cursor.get_data::<Vec<u8>>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => value,
                            }
                        }
                        SqlDataType::Numeric => {
                            let value = cursor.get_data::<&str>(i as u16)?;
                            match value {
                                None => {
                                    nulls[(i - 1) as usize] = true;
                                    vec![]
                                }
                                Some(value) => {
                                    let num = u128::from_str(value)?;
                                    let exp = match col_type.scale {
                                        None => 0,
                                        Some(exp) => exp,
                                    };
                                    let mul = 10_u128.pow(exp as u32);
                                    let unscaled = num * mul;
                                    let mut unscaled_bytes = unscaled.to_be_bytes();

                                    let mut unscaled_bytes: Vec<u8> = unscaled_bytes
                                        .iter()
                                        .rev()
                                        .skip_while(|b| **b == 0)
                                        .map(|b| *b)
                                        .collect();

                                    unscaled_bytes.reverse();

                                    let byte_len = unscaled_bytes.len();
                                    let mut padded_bytes =
                                        vec![0; (col_type.width as usize - byte_len) as usize];
                                    padded_bytes.extend_from_slice(&unscaled_bytes);

                                    if num < 0 {
                                        negate(
                                            &mut padded_bytes,
                                            (col_type.width as usize - byte_len),
                                        );
                                    }

                                    let mut final_bytes: Vec<u8> = vec![];

                                    for i in 0..(padded_bytes.len() / 8) {
                                        let chunk =
                                            &padded_bytes[(i as usize) * 8..(i as usize + 1) * 8];
                                        for byte in chunk.iter().rev() {
                                            final_bytes.push(*byte);
                                        }
                                    }

                                    final_bytes
                                }
                            }
                        }
                    };

                    &values.push(byte_val);
                }

                let bitmap = create_nulls_bitmap(cols, &nulls);

                let row_size: u32 =
                    bitmap.len() as u32 + (&values).iter().fold(0, |acc, x| acc + x.len()) as u32;

                let flattened_values = (&values)
                    .into_iter()
                    .flatten()
                    .map(|v| *v)
                    .collect::<Vec<u8>>();

                let nl = nulls
                    .iter()
                    .map(|n| if *n { "t".to_string() } else { "f".to_string() })
                    .collect::<Vec<String>>()
                    .concat();

                let bm = bitmap
                    .iter()
                    .map(|b| format!("{:b}", b))
                    .collect::<Vec<String>>()
                    .concat();

                println!("{}", nl);
                println!("{}", bm);

                println!("nulls: {}", nulls.len());
                println!("bitma: {}", bitmap.len());
                println!("c/8: {}", column_types.len() / 8);

                output_file.write(&row_size.to_le_bytes());
                output_file.write(&bitmap.as_slice());
                output_file.write_all(&flattened_values);

                values = vec![];

                rows_written += 1;
            }
        }
    };

    Ok(())
}

fn negate(bytes: &mut [u8], head: usize) {
    for i in 0..head {
        bytes[i] ^= 0xFF;
    }
}

fn create_nulls_bitmap(cols: i16, nulls: &Vec<bool>) -> Vec<u8> {
    let multiplier = size_of::<u8>() as i16 * 8;
    let bytes_needed = cols / multiplier + if cols % multiplier != 0 { 1 } else { 0 };

    let mut bitmap: Vec<u8> = vec![];

    for i in 0..(nulls.len() / 8) {
        let mut byte = 0_u8;
        let subset = &nulls[(i * 8)..((i + 1) * 8)];

        for i in 0..subset.len() {
            if subset[i] {
                byte |= (1 << (i as i8 - 7).abs() as u8);
            }
        }

        bitmap.push(byte);
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
