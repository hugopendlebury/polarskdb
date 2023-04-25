use std::fmt::Display;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DBError {
    QueryError,
    ExecuteError,
    RawCmdError,
    ConnectError,
    PolarsCreationError,
}

impl ToPyObject for DBError {
    fn to_object(&self, py: Python) -> PyObject {
        self.to_string().to_object(py)
    }
}

impl<'a> FromPyObject<'a> for DBError {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let s = ob.extract::<String>()?;
        match s.as_str() {
            "QueryError" => Ok(DBError::QueryError),
            "ExecuteError" => Ok(DBError::ExecuteError),
            "RawCmdError" => Ok(DBError::RawCmdError),
            "ConnectError" => Ok(DBError::ConnectError),
            "PolarsCreationError" => Ok(DBError::PolarsCreationError),
            _ => Err(PyTypeError::new_err(format!(
                "Cannot convert {} to DBError",
                s
            ))),
        }
    }
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            DBError::QueryError => "QueryError".to_string(),
            DBError::ExecuteError => "ExecuteError".to_string(),
            DBError::RawCmdError => "RawCmdError".to_string(),
            DBError::ConnectError => "ConnectError".to_string(),
            DBError::PolarsCreationError => "olarsCreationError".to_string(),
        };
        write!(f, "{}", v)
    }
}

#[pyclass(name = "PySQLXError", extends = PyTypeError)]
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PySQLXError {
    pub code: String,
    pub message: String,
    pub error: DBError,
}

#[pymethods]
impl PySQLXError {
    #[new]
    pub fn py_new(code: String, message: String, error: DBError) -> Self {
        Self {
            code,
            message,
            error,
        }
    }

    pub fn __str__(&self) -> String {
        format!(
            "PySQLXError(code='{}', message='{}', error='{}')",
            self.code, self.message, self.error
        )
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn code(&self) -> String {
        self.code.clone()
    }

    pub fn message(&self) -> String {
        self.message.clone()
    }

    pub fn error(&self) -> String {
        self.error.to_string()
    }
}

impl PySQLXError {
    pub fn new(code: String, message: String, error: DBError) -> Self {
        Self {
            code,
            message,
            error,
        }
    }
    pub fn to_pyerr(&self) -> PyErr {
        PyErr::new::<PySQLXError, _>((
            self.code.clone(),
            self.message.clone(),
            self.error.to_string(),
        ))
    }
}

pub fn py_error(err: String, typ: DBError) -> PySQLXError {
    PySQLXError::py_new(String::from("0"), String::from(err.to_string()), typ)
}
