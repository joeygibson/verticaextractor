use core::fmt;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Errors {
    TableNotFoundError,
}

impl Error for Errors {}

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error: {:?}", self)
    }
}
