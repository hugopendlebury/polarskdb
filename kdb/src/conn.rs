use core::result::Result as StdResult;
use kdbplus::ipc::K;
use kdbplus::ipc::*;
use kdbplus::*;
use polars_core::prelude::*;
use py_types::{py_error, DBError, PySQLXError};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use core::result::Result;
use super::helpers::*;
use rayon::prelude::*;

#[pyclass]
#[derive(Debug, Clone)]
pub struct Connection {
    //conn: QStream,
    //hostname: String,
}


#[derive(Debug)]
pub enum PolarsConversionError{
    UnableToCreateIterator
}

pub struct PolarsUtils {
    //df: DataFrame
}

impl PolarsUtils {



    pub fn new() -> Result<Self, PolarsConversionError> {
        Ok(Self { })
    }
    
    fn series_to_k<'a, 'b, F, NewK, T, ListK>(&self, series: &'a Series, extractor: F, new_k: NewK, list_k: ListK) -> K 
    where    F: Fn(&'a Series) -> Box<dyn PolarsIterator<Item = Option<T>> + 'a>  + 'a
            ,NewK: Fn(T) -> K
            ,ListK: Fn(Vec<K>) -> K
            ,T: num_traits::Num + Send + 'b 

    {
        let chunk = extractor(series);
        let x = chunk.into_iter();
        let results : Vec<K> = x.map( |x| {
            match x {
                Some(a) =>  { return new_k(a) ;}
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect();

        let k = list_k(results);

        return k;
    }


    pub fn to_k<'a>(&self, dataframe: &DataFrame) -> K {

        let columns = dataframe.get_columns().par_iter().map(|_col| {

            let dtype = _col._dtype();
            match dtype {
                DataType::Utf8 => {
                    let x = _col.utf8().unwrap().into_iter();
                    let converted: Vec<K> = x.map( |x| {
                        match x {
                            Some(a) =>  { return K::new_symbol(String::from(a)); }
                            None => { return K::new_null();}
                        }  ;
                    }).into_iter().collect();
                    K::new_compound_list(converted)
                }
                DataType::Float64 => {
                    self.series_to_k(_col, 
                        |s| s.f64().unwrap().into_iter(), 
                        |v| K::new_float(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int32  => {
                    self.series_to_k(_col, 
                        |s| s.i32().unwrap().into_iter(), 
                        |v| K::new_int(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int64 => {
                    self.series_to_k(_col, 
                        |s| s.i64().unwrap().into_iter(), 
                        |v| K::new_long(v),
                        |k| K::new_compound_list(k)
                    )
                }
                _ => panic!()
            }
    
        }).collect();

        return K::new_compound_list(columns);
    }
}



impl Connection {




    pub async fn new(hostname: String) -> StdResult<Self, PySQLXError> {
        let conn = match kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, hostname.as_str(), 5001_u16, "").await {
            Ok(r) => r,
            Err(e) => return Err(py_error(e.to_string(), DBError::ConnectError)),
        };
        Ok(Self { })
    }

    async fn _query(&self, sql: String) -> StdResult<DataFrame, PySQLXError> {
        let conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await;
        let query = &sql.as_str();
        match conn.unwrap().send_sync_message(query).await {
            Ok(r) => {
                let polars_columns = k_result_to_series(&r);
                match DataFrame::new(polars_columns) {
                    Ok(r) => {
                        return Ok(r)
                    },
                    Err(e) => {
                        return Err(py_error(
                            String::from("Unable to create dataframe"),
                            DBError::PolarsCreationError,
                        ))
                    }
                }
            },
            Err(e) => {
                return Err(py_error(
                    String::from("Unable to execute query"),
                    DBError::QueryError,
                ))
            }
        }

    }

    //TODO - REFACTOR _query to use a union Type
    async fn _send_k(&self, k: &K) -> StdResult<(), PySQLXError> {
        let conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await;

        match conn.unwrap().send_sync_message(k).await {
            Ok(r) => {
                Ok(())
            },
            Err(e) => {
                return Err(py_error(
                    String::from("Unable to execute query"),
                    DBError::QueryError,
                ))
            }
        }

    }

}

#[pymethods]
impl Connection {

    pub fn query<'a>(&self, py: Python<'a>, sql: String) -> PyResult<&'a PyAny> {

        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._query(sql).await {
                Ok(r) => Ok(PyDataFrame(r)),
                Err(e) => Err(e.to_pyerr()),
            }
        })
    }


    pub fn polars_to_table<'a>(&self, py: Python<'a>, table_name: String, data: PyDataFrame) -> PyResult<&'a PyAny> {

        //let mut file = File::options().append(true).open("/Users/hugo/polars.log")?;

        let df = data.0;

        let utils = PolarsUtils::new();
        let util_unwrapped = utils.unwrap();
        let values = util_unwrapped.to_k(&df);
        let keys = K::new_symbol_list(
                        df.get_column_names().into_iter().map(|col| String::from(col)).collect()
                        ,qattribute::NONE
                    );

        let dictionary = K::new_dictionary(keys, values).unwrap();
        let table = dictionary.flip().unwrap();
        let table_assign=K::new_compound_list(vec![K::new_string(String::from("set"), qattribute::NONE), K::new_symbol(table_name.clone()), table]);
        
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._send_k(&table_assign).await {
                Ok(_r) => Ok(true),
                Err(e) => Err(e.to_pyerr()),
            }
        })

    }

    //TODO: Implement this
    pub fn is_healthy(&self) -> bool {
        true
        //self.conn.is_healthy()
    }
}
