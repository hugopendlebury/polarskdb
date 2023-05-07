use chrono::prelude::*;
use kdbplus::ipc::K;
use kdbplus::*;
use once_cell::sync::Lazy;
use polars::export::chrono::NaiveDate;
use polars::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::prelude::*;

//use chrono
    const KDB_EPOCH : Lazy<chrono::DateTime<Utc>> = Lazy::new(|| Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
    //const KDB_EPOCH: chrono::DateTime<Utc> = ;
    //const UNIX_EPOCH: Lazy<chrono::DateTime<Utc>> = Lazy::new(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
    const TIME_OFFSET: Lazy<i64> = Lazy::new(|| {
        KDB_EPOCH.signed_duration_since(
            Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()).num_seconds()
    });
    const SECONDS_IN_DAY: i32 = 60 * 60 * 24;
    const NANO_SECONDS_IN_SECOND: i32 = 1000000000;


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
    //Below is from k.h
    /*
    #define KP 12 // 8 timestamp long   kJ (nanoseconds from 2000.01.01)
    #define KM 13 // 4 month     int    kI (months from 2000.01.01)
    #define KD 14 // 4 date      int    kI (days from 2000.01.01)
    */

    let mut file = File::options().append(true).open("/Users/hugo/polars.log").unwrap();
    file.write_all(b"Entered conversion\n").unwrap();

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
                qtype::DATE_LIST => Series::new(
                    col.as_str(),
                    c.as_vec::<i32>()
                        .unwrap()
                        .iter()
                        .map(|d| {
                            file.write_all(b"Got value").unwrap();
                            file.write_all(d.to_be_bytes()).unwrap();
                            Utc.timestamp_opt(
                                (Into::<i64>::into(d * SECONDS_IN_DAY) + *TIME_OFFSET)
                                    .into(),
                                0,
                            )
                            .unwrap()
                            .date_naive()
                        })
                        .collect::<Vec<NaiveDate>>(),
                ),
                _ => panic!(),
            }
        })
        .collect()
}
