use regex;
use regex::Regex;

use lazy_static::lazy_static;

#[derive(Debug, PartialEq)]
pub enum SqlDataType {
    Integer,
    Float,
    Char,
    Varchar,
    Boolean,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Varbinary,
    Binary,
    Numeric,
    Interval,
}

impl SqlDataType {
    pub fn from_string(string: &str) -> SqlDataType {
        lazy_static! {
            static ref PAREN_REGEX: Regex = Regex::new(r"\(.+\)").unwrap();
        }

        let no_parens = PAREN_REGEX.replace(string, "");

        match no_parens.to_lowercase().as_str() {
            "int" => SqlDataType::Integer,
            "float" => SqlDataType::Float,
            "char" => SqlDataType::Char,
            "varchar" => SqlDataType::Varchar,
            "boolean" => SqlDataType::Boolean,
            "date" => SqlDataType::Date,
            "timestamp" => SqlDataType::Timestamp,
            "timestamptz" => SqlDataType::TimestampTz,
            "time" => SqlDataType::Time,
            "timetz" => SqlDataType::TimeTz,
            "varbinary" => SqlDataType::Varbinary,
            "binary" => SqlDataType::Binary,
            "numeric" => SqlDataType::Numeric,
            "interval" => SqlDataType::Interval,
            _ => panic!("unknown data type"),
        }
    }
}
