use core::result::Result as StdResult;
use kdbplus::ipc::K;
use kdbplus::ipc::*;
use kdbplus::*;
use num_traits::ToPrimitive;
use polars_core::prelude::*;
use py_types::{py_error, DBError, PySQLXError};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use core::result::Result;
use super::helpers::*;
use std::fs::File;
use std::io::prelude::*;

#[pyclass]
#[derive(Debug, Clone)]
pub struct Connection {
    //conn: QStream,
    //hostname: String,
}
/* 
pub struct IterOverVecVec<'a> {
    m: &'a dyn IterTrait
}

pub trait IterTrait {}
impl dyn IterTrait + '_ {
    pub fn get_iter<'a>(&'a self) -> IterOverVecVec<'a> {
        IterOverVecVec::new(self)
    }
}
*/



struct BoxedPolarsIteratorLike<T> {
    iterator: Box<dyn PolarsIterator<Item = Option<T>>>
}


#[derive(Debug)]
pub enum PolarsConversionError{
    UnableToCreateIterator
}

pub struct PolarsUtils {
    //df: DataFrame
}

//struct BoxedConnectionLike(Box<dyn ConnectionLike>);


impl <T: num_traits::Num + Copy> BoxedPolarsIteratorLike<T>{

    pub fn new(iterator: Box<dyn PolarsIterator<Item = Option<T>>>) -> Result<Self, PolarsConversionError> {
        Ok(Self { iterator })
    }

    pub fn to_k<F>(column_values: &mut Box<dyn PolarsIterator<Item = Option<T>>>, new_k: F,  ) -> Vec<K>
    where    F: Fn(T) -> K
            ,T: num_traits::Num + Copy
    {


        column_values.map( |x| {
            match x {
                Some(a) =>  { return new_k(a); }
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect()

    }



}

impl PolarsUtils {



    pub fn new() -> Result<Self, PolarsConversionError> {
        Ok(Self { })
    }
     
    fn get_col_as_k_vec<'a, 'b ,'c, T, F>(&self, column_values: &'a mut Box<dyn PolarsIterator<Item = Option<T>>>,  new_k: F ) -> Vec<K>
    where    F: Fn(T) -> K
            ,T: num_traits::Num + Copy + 'a
    {

        column_values.map( move |x| {
            match x {
                Some(a) =>  { return new_k(a); }
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect()
    
    }
    
    fn series_to_k_3<'a, 'b, F, NewK, T>(series: &'a Series, extractor: F, new_k: NewK) -> Vec<K> 
    where    F: Fn(&'a Series) -> Box<dyn PolarsIterator<Item = Option<T>>>  
        ,NewK: Fn(T) -> K
        ,T: num_traits::Num + 'b 

    {
        let chunk = extractor(series);
        let x = chunk.into_iter();
        let results : Vec<K> = x.map( |x| {
            match x {
                Some(a) =>  { return new_k(a) ;}
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect();
        return results;
    }

    fn series_to_k2<'a, 'b, F, NewK, T, ListK>(&self, series: &'a Series, extractor: F, new_k: NewK, list_k: ListK) -> K 
    where    F: Fn(&'a Series) -> Box<dyn PolarsIterator<Item = Option<T>> + 'a>  + 'a
            ,NewK: Fn(T) -> K
            ,ListK: Fn(Vec<K>) -> K
            ,T: num_traits::Num + 'b 

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


    fn series_to_kx<'a, 'b, F, NewK, T>(&self, series: &'a Series, extractor: F, new_k: NewK) -> ()
    where    F: Fn(&'a Series) -> Result<&ChunkedArray<T>, PolarsError> + 'a
            , NewK: Fn(T) -> K
            ,T: PolarsDataType + num_traits::Num + 'a

    {
        let chunk = extractor(series);
        let x = chunk.unwrap().chunks().into_iter();
        //let b = (&(**x)).as_any().downcast_ref::<PrimitiveArray<f64>>();
        /* 
        let r = x.map(|a| {
           //let x =  **a;
            let b = a.as_any().downcast_ref::<PrimitiveArray<i64>>();
        });
        */
        /* 
        //x.into_iter();
        let results : Vec<K> = x.map( |x| {
            match x {
                Some(a) =>  { return new_k(a) ;}
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect();
        return results;
        */
    }

/* 
    fn series_to_k<'a, F, NewK, T>(&self, series: &'a Series, extractor: F, new_k : NewK) -> () 
    where    F: Fn(&'a Series) -> 
            ,T: PolarsDataType + num_traits::Num 
            //,New_K: Fn(&'a T) -> K
    {
        let chunk = extractor(series);
        let x = chunk.into_iter();
        let converted = x.map( |x| {
            match x {
                Some(a) =>  { return K::new_float(a.to_i64()); }
                None => { return K::new_null();}
            }  ;
        }).into_iter();
    }
*/

    pub fn to_k<'a>(&self, dataframe: &DataFrame) -> K {

        //let a = for<T> |s: &str, t: T| {...}

        let mut columns = Vec::<K>::new();

        for _col in dataframe.get_columns() {
    
            let dtype = _col._dtype();
            if *dtype == DataType::Utf8 {
                let x = _col.utf8().unwrap().into_iter().peekable();
                let converted: Vec<K> = x.map( |x| {
                    match x {
                        Some(a) =>  { return K::new_symbol(String::from(a)); }
                        None => { return K::new_null();}
                    }  ;
                }).into_iter().collect();
                columns.push(K::new_compound_list(converted));

            }
            if *dtype == DataType::Float64 {
                    let col_data = self.series_to_k2(_col, 
                        |s| s.f64().unwrap().into_iter(), 
                        |v| K::new_float(v),
                        |k| K::new_compound_list(k)
                    );
                    columns.push(col_data);
            }
            if *dtype == DataType::Int32 {
                let col_data = self.series_to_k2(_col, 
                    |s| s.i32().unwrap().into_iter(), 
                    |v| K::new_int(v),
                    |k| K::new_compound_list(k)
                );
                columns.push(col_data);
            }
            if *dtype == DataType::Int64 {
                let col_data = self.series_to_k2(_col, 
                    |s| s.i64().unwrap().into_iter(), 
                    |v| K::new_long(v),
                    |k| K::new_compound_list(k)
                );
                columns.push(col_data);
            }
    
        }

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
        let mut conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await;
        let query = &sql.as_str();
        match conn.unwrap().send_sync_message(query).await {
            Ok(r) => {
                let polarsColumns = k_result_to_series(&r);
                match DataFrame::new(polarsColumns) {
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
        let mut conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await;

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
    //'a: 'b, 'b
    pub fn query<'a>(&self, py: Python<'a>, sql: String) -> PyResult<&'a PyAny> {
        let mut file = File::options().append(true).open("/Users/hugo/polars.log")?;
        file.write_all(b"Entered python\n")?;
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._query(sql).await {
                Ok(r) => Ok(PyDataFrame(r)),
                Err(e) => Err(e.to_pyerr()),
            }
        })
    }


    pub fn polars_to_table<'a>(&self, py: Python<'a>, table_name: String, data: PyDataFrame) -> PyResult<&'a PyAny> {

        let mut file = File::options().append(true).open("/Users/hugo/polars.log")?;
        file.write_all(b"Entered polars to table\n")?;

        let df = data.0;

        let utils = PolarsUtils::new();
        let util_unwrapped = utils.unwrap();
        let values = util_unwrapped.to_k(&df);
        let keys = K::new_symbol_list(
                        df.get_column_names().into_iter().map(|col| String::from(col)).collect()
                        ,qattribute::NONE
                    );

        file.write_all(b"Got Column Names\n")?;

        file.write_all(b"Got Keys\n")?;
        let dictionary = K::new_dictionary(keys, values).unwrap();
        file.write_all(b"Created Dictionary\n")?;
        let table = dictionary.flip().unwrap();
        file.write_all(b"Got table\n")?;
        let mut table_assign=K::new_compound_list(vec![K::new_string(String::from("set"), qattribute::NONE), K::new_symbol(table_name.clone()), table]);
        
        file.write_all(b"About to write table\n")?;
 
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._send_k(&table_assign).await {
                Ok(r) => Ok(true),
                Err(e) => Err(e.to_pyerr()),
            }
        })

    }
/* 
    pub async fn polars_to_table<'a>(&self, py: Python<'a>, table_name: String, data: PyDataFrame) -> bool {

        let df = data.0;


        for _col in df.get_columns() {

            let dtype = _col._dtype();
            if *dtype == DataType::Utf8 {
                let x = _col.utf8().unwrap();
                //x.as_vec::<String>().unwrap();
                //let as_vec: Vec<Option<i32>> = s.i32()?.into_iter().collect();

                // if we are certain we don't have missing values
                let as_vec: Vec<String> = _col.utf8().unwrap().into_no_null_iter().map(|c| String::from(c)).collect();

                let symbol_list = K::new_symbol_list(
                    as_vec,
                    qattribute::NONE,
                );


                let keys = K::new_symbol_list(
                    vec![
                        String::from("people"),
                    ],
                    qattribute::NONE,
                );
                let values = K::new_compound_list(vec![symbol_list]);
                let dictionary = K::new_dictionary(keys, values).unwrap();
                let table = dictionary.flip().unwrap();
                let mut conn = kdbplus::ipc::QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await;
                conn.unwrap().send_sync_message(&table).await;
            }

        }


        return true;
    }
*/
    pub fn is_healthy(&self) -> bool {
        return true;
        //self.conn.is_healthy()
    }
}
