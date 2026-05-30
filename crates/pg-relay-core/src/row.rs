use crate::{Error, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// A SQL-shaped value. Kept narrow on purpose — add variants
/// only when a real use case shows up, not speculatively.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum Column {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(DateTime<Utc>),
    Json(serde_json::Value),
}

impl Column {
    pub fn type_name(&self) -> &'static str {
        match self {
            Column::Null => "null",
            Column::Bool(_) => "bool",
            Column::Int64(_) => "int64",
            Column::Float64(_) => "float64",
            Column::Text(_) => "text",
            Column::Bytes(_) => "bytes",
            Column::Date(_) => "date",
            Column::Timestamp(_) => "timestamp",
            Column::Json(_) => "json",
        }
    }
}

macro_rules! impl_from_for_column {
    ($t:ty => $variant:ident) => {
        impl From<$t> for Column {
            fn from(v: $t) -> Self {
                Column::$variant(v)
            }
        }
    };
}

impl_from_for_column!(bool => Bool);
impl_from_for_column!(i64 => Int64);
impl_from_for_column!(f64 => Float64);
impl_from_for_column!(String => Text);
impl_from_for_column!(Vec<u8> => Bytes);
impl_from_for_column!(NaiveDate => Date);
impl_from_for_column!(DateTime<Utc> => Timestamp);
impl_from_for_column!(serde_json::Value => Json);

impl From<&str> for Column {
    fn from(v: &str) -> Self {
        Column::Text(v.to_string())
    }
}

impl<T> From<Option<T>> for Column
where
    T: Into<Column>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Column::Null,
        }
    }
}

/// A single row of result data. Columns are positional; the
/// names live in the table function's schema, not on the row.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<Column>,
}

impl Row {
    pub fn new() -> Self {
        Row::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Row {
            columns: Vec::with_capacity(n),
        }
    }

    pub fn push(mut self, c: impl Into<Column>) -> Self {
        self.columns.push(c.into());
        self
    }
}

/// A bag of named input values. Constructed by the framework
/// from the caller's parameters; user code reads them via the
/// typed getters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Inputs {
    pub values: Vec<(String, Column)>,
}

impl Inputs {
    pub fn new() -> Self {
        Inputs::default()
    }

    pub fn from_pairs<I, S>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (S, Column)>,
        S: Into<String>,
    {
        Inputs {
            values: pairs.into_iter().map(|(n, c)| (n.into(), c)).collect(),
        }
    }

    pub fn get(&self, name: &str) -> Result<&Column> {
        self.values
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
            .ok_or_else(|| Error::MissingInput(name.to_string()))
    }

    pub fn get_i64(&self, name: &str) -> Result<i64> {
        match self.get(name)? {
            Column::Int64(v) => Ok(*v),
            other => Err(Error::InputType {
                name: name.to_string(),
                expected: "int64",
                got: other.type_name(),
            }),
        }
    }

    pub fn get_text<'a>(&'a self, name: &str) -> Result<&'a str> {
        match self.get(name)? {
            Column::Text(v) => Ok(v.as_str()),
            other => Err(Error::InputType {
                name: name.to_string(),
                expected: "text",
                got: other.type_name(),
            }),
        }
    }

    pub fn get_date(&self, name: &str) -> Result<NaiveDate> {
        match self.get(name)? {
            Column::Date(v) => Ok(*v),
            other => Err(Error::InputType {
                name: name.to_string(),
                expected: "date",
                got: other.type_name(),
            }),
        }
    }

    pub fn get_opt_text<'a>(&'a self, name: &str) -> Result<Option<&'a str>> {
        match self.get(name)? {
            Column::Null => Ok(None),
            Column::Text(v) => Ok(Some(v.as_str())),
            other => Err(Error::InputType {
                name: name.to_string(),
                expected: "text or null",
                got: other.type_name(),
            }),
        }
    }
}
