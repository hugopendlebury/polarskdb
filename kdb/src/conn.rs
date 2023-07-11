use core::result::Result as StdResult;
use kdbplus::ipc::K;
use kdbplus::ipc::*;
use kdbplus::*;
use polars::prelude::*;
use log::info;
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
    hostname: String,
    port: u16,
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
    
    fn series_to_k_par<'a, F, NewK, T, ListK>(&self, iterator: F, new_k: NewK, list_k: ListK) -> K 
    where    F: IntoParallelIterator<Item = Option<T>> 
            ,NewK: Fn(T) -> K + std::marker::Sync
            ,ListK: Fn(Vec<K>) -> K
            ,T: num_traits::Num

    {

        let results = iterator.into_par_iter().map( | x| {
            match x {
                Some(a) =>  new_k(a),
                None => K::new_null()
            }  
        }).collect();

        let k = list_k(results);

        return k;
    }
    
    pub fn to_k<'a>(&self, dataframe: &DataFrame) -> K {

        let columns = dataframe.get_columns().into_iter().map(|series| -> K {
            info!("in to_k with dtype = {}", series.dtype());
            match series.dtype() {

                DataType::Utf8 => {
                    let x = series.utf8().unwrap().par_iter();
                    let converted: Vec<K> = x.map( |x| {
                        match x {
                            Some(a) =>  K::new_symbol(String::from(a)),
                            None => K::new_null()
                        }  
                    }).collect();
                    K::new_compound_list(converted)
                }
                DataType::Float32 => {
                    //NOTE - Polars doesn't support IntoParallelIterator for all types of ChunkedArray 
                    //It might come at some point but for now using into_iter with collect
                    self.series_to_k_par(series.f32().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_real(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Float64 => {
                    self.series_to_k_par( 
                        series.f64().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_float(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int8 => {
                    //KDB Does have an Int8 - Assign it to an i16
                    self.series_to_k_par(series.i8().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_short(v.into()),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int16 => {
                    self.series_to_k_par(series.i16().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_short(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int32  => {
                    self.series_to_k_par (series.i32().unwrap().into_iter().collect::<Vec<_>>(),
                        |v| K::new_int(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Int64 => {
                    self.series_to_k_par(series.i64().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_long(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::UInt8 => {
                    self.series_to_k_par(series.u8().unwrap().into_iter().collect::<Vec<_>>(), 
                        |v| K::new_byte(v),
                        |k| K::new_compound_list(k)
                    )
                }
                DataType::Date => {
                    let x = series.date().unwrap().into_iter();
                    let converted: Vec<K> = x.map( |x| {
                        match x {
                            Some(a) =>  {
                                //Polars gives us the number of days from the Epoch
                                let dt = *UNIX_EPOCH_DATE + chrono::Duration::days(a as i64);
                                K::new_date(dt.date_naive())
                            },
                            None => K::new_null()
                        }  
                    }).collect();
                    K::new_compound_list(converted)
                }
                DataType::Datetime(TimeUnit::Microseconds,_)=> {
                    let x = series.datetime().unwrap().into_iter();
                    let converted: Vec<K> = x.map( |x| {
                        info!("map value {:?}", x);
                        match x {
                            Some(a) =>  {
                                let dt = *UNIX_EPOCH_DATE + chrono::Duration::microseconds(a);
                                info!("date is {}", dt);
                                K::new_datetime(dt)
                            },
                            None => K::new_null()
                        }  
                    }).collect();
                    K::new_compound_list(converted)
                }
                DataType::Datetime(TimeUnit::Milliseconds,_)=> {
                    let x = series.datetime().unwrap().into_iter();
                    let converted: Vec<K> = x.map( |x| {
                        info!("map value {:?}", x);
                        match x {
                            Some(a) =>  {
                                let dt = *UNIX_EPOCH_DATE + chrono::Duration::milliseconds(a);
                                info!("date is {}", dt);
                                K::new_datetime(dt)
                            },
                            None => K::new_null()
                        }  
                    }).collect();
                    K::new_compound_list(converted)
                }
                _ => panic!()
            }
    
        }).collect();
        
        return K::new_compound_list(columns);
    }
}



impl Connection {




    pub async fn new(hostname: String, port: u16) -> StdResult<Self, PySQLXError> {
        let conn = match kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, hostname.as_str(), port, "").await {
            Ok(r) => r,
            Err(e) => return Err(py_error(e.to_string(), DBError::ConnectError)),
        };
        Ok(Self { hostname : hostname, port:port})
    }

    async fn _query(&self, sql: String) -> StdResult<DataFrame, PySQLXError> {
        let conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, &self.hostname, self.port, "").await;
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
                            format!("Unable to create dataframe {}", e),
                            DBError::PolarsCreationError,
                        ))
                    }
                }
            },
            Err(e) => {
                return Err(py_error(
                    format!("Unable to execute query {}", e),
                    DBError::QueryError,
                ))
            }
        }

    }

    //TODO - REFACTOR _query to use a union Type
    async fn _send_k(&self, k: &K) -> StdResult<(), PySQLXError> {
        let conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, &self.hostname, self.port, "").await;

        match conn.unwrap().send_async_message(k).await {
            Ok(_) => {
                Ok(())
            },
            Err(e) => {
                return Err(py_error(
                    format!("Unable to execute query {}", e),
                    DBError::QueryError,
                ))
            }
        }

    }

}

#[pymethods]
impl Connection {

    pub fn list_tables<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {

        let query = "( [] table_names: tables[])";
        return self.query(py, query.to_string());

    }


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
        info!("Saving to table");
        let df = data.0;

        let utils = PolarsUtils::new();
        let util_unwrapped = utils.unwrap();
        info!("About to call to_k");
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
                Ok(_) => Ok(true),
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
