use crate::sql_data_type::SqlDataType;

#[derive(Debug)]
pub struct ColumnType {
    name: String,
    pub(crate) data_type: SqlDataType,
    pub(crate) width: u16,
    pub(crate) precision: Option<u16>,
    pub(crate) scale: Option<u16>,
}

impl ColumnType {
    pub fn new(values: &Vec<String>) -> ColumnType {
        let scale = if values[4].is_empty() {
            None
        } else {
            let scale = values[4].parse::<u16>().unwrap();
            Some(scale)
        };

        let precision = if !values[3].is_empty() {
            Some(values[3].parse::<u16>().unwrap())
        } else if !values[5].is_empty() {
            Some(values[5].parse::<u16>().unwrap())
        } else if !values[6].is_empty() {
            Some(values[6].parse::<u16>().unwrap())
        } else {
            None
        };

        ColumnType {
            name: values[0].clone(),
            data_type: SqlDataType::from_string(values[1].clone().as_str()),
            width: values[2].parse::<u16>().unwrap(),
            precision,
            scale,
        }
    }
}
