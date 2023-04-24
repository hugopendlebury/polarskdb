use kdb::Connection;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use py_types::{PySQLXError};
use polars_core::prelude::*;
use kdbplus::ipc::K;
use kdbplus::ipc::*;
use kdbplus::*;


pub fn get_version() -> String {
    let version = env!("CARGO_PKG_VERSION").to_string();
    // cargo uses "1.0-alpha1" etc. while python uses "1.0.0a1", this is not full compatibility,
    // but it's good enough for now
    // see https://docs.rs/semver/1.0.9/semver/struct.Version.html#method.parse for rust spec
    // see https://peps.python.org/pep-0440/ for python spec
    // it seems the dot after "alpha/beta" e.g. "-alpha.1" is not necessary, hence why this works
    version.replace("-alpha", "a").replace("-beta", "b")
}

pub fn main() {
    let df = df!("Fruit" => &["Apple", "Apple", "Pear"]).unwrap();
    polars_to_table(String::from("dummy_hugo"), df);
    println!("Yo im done")
}

pub async fn polars_to_table<'a>(table_name: String, data: DataFrame) -> bool {

    let df = data;


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

#[pyfunction]
fn new(py: Python, hostname: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        match Connection::new(hostname).await {
            Ok(r) => Ok(r),
            Err(e) => Err(e.to_pyerr()),
        }
    })
}

#[pymodule]
fn polarskdb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", get_version())?;
    m.add_function(wrap_pyfunction!(new, m)?)?;
    m.add_class::<Connection>()?;
    //m.add_class::<PySQLXResult>()?;
    m.add_class::<PySQLXError>()?;
    Ok(())
}