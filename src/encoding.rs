use crate::column_type::ColumnType;

pub fn encode_value<T>(value: T, col_type: &ColumnType) -> Vec<u8> {
    // for any of the `var*` types, prepend a 4-byte value indicating the length

    vec![]
}
