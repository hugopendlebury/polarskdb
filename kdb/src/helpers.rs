use chrono::prelude::*;
use kdbplus::ipc::K;
use kdbplus::*;
use polars_core::prelude::*;
use rayon::prelude::*;
//use chrono

pub fn get_column_names<'a>(result: &'a K) -> impl Iterator<Item = &'a String> {
    let dictionary = result.get_dictionary().unwrap().as_vec::<K>().unwrap();
    let columns = dictionary[0].as_vec::<String>().unwrap();
    columns.iter()
}

pub fn get_column_names_as_vec<'a>(result: &'a K) -> &Vec<String> {
    let dictionary = result.get_dictionary().unwrap().as_vec::<K>().unwrap();
    dictionary[0].as_vec::<String>().unwrap()
}

pub fn k_result_to_series(result: &K) -> Vec<Series> {

    get_column_names_as_vec(result)
        .par_iter()
        .map(|col| {
            let c = result.get_column(col).unwrap();
            match c.get_type() {
                qtype::LONG_LIST => Series::new(col.as_str(), c.as_vec::<i64>().unwrap()),
                qtype::SYMBOL_LIST => Series::new(col.as_str(), c.as_vec::<String>().unwrap()),
                qtype::FLOAT_LIST => Series::new(col.as_str(), c.as_vec::<f64>().unwrap()),
                qtype::INT_LIST => Series::new(col.as_str(), c.as_vec::<i32>().unwrap()),
                qtype::REAL_LIST => Series::new(col.as_str(), c.as_vec::<f32>().unwrap()),
                qtype::BOOL_LIST => Series::new(col.as_str(), c.as_vec::<bool>().unwrap()),
                //qtype::BYTE_LIST => {
                //polars_columns.push(Series::new(col.as_str(), c.as_vec::<u8>().unwrap()))
                //}
                _ => panic!(),
            }
        })
        .collect()
}
