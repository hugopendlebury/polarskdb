use chrono::prelude::*;
use kdbplus::ipc::K;
use kdbplus::*;
use log::info;
use once_cell::sync::Lazy;
use polars::export::chrono::NaiveDate;
use polars::prelude::*;
use rayon::prelude::*;

//use chrono
pub const UNIX_EPOCH_DATE: Lazy<chrono::DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
pub const KDB_EPOCH: Lazy<chrono::DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
const TIME_OFFSET: Lazy<i64> = Lazy::new(|| {
    KDB_EPOCH
        .signed_duration_since(Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap())
        .num_seconds()
});
const SECONDS_IN_DAY: i32 = 60 * 60 * 24;
const NANO_SECONDS_IN_SECOND: i64 = 1000000000;

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
                qtype::SHORT_LIST => Series::new(col.as_str(),
                    c.as_vec::<i16>()
                        .unwrap()
                        .iter()
                        .map(|d| {
                            i32::from(*d)
                        }).collect::<Vec<i32>>()
                ),
                qtype::INT_LIST => Series::new(col.as_str(), c.as_vec::<i32>().unwrap()),
                qtype::LONG_LIST => Series::new(col.as_str(), c.as_vec::<i64>().unwrap()),
                qtype::SYMBOL_LIST => Series::new(col.as_str(), c.as_vec::<String>().unwrap()),
                qtype::REAL_LIST => Series::new(col.as_str(), c.as_vec::<f32>().unwrap()),
                qtype::FLOAT_LIST => Series::new(col.as_str(), c.as_vec::<f64>().unwrap()),
                qtype::BOOL_LIST => Series::new(col.as_str(), c.as_vec::<bool>().unwrap()),
                qtype::DATE_LIST => Series::new(
                    col.as_str(),
                    c.as_vec::<i32>()
                        .unwrap()
                        .iter()
                        .map(|d| {
                            Utc.timestamp_opt(
                                (Into::<i64>::into(d * SECONDS_IN_DAY) + *TIME_OFFSET).into(),
                                0,
                            ).unwrap().date_naive()
                        })
                        .collect::<Vec<NaiveDate>>(),
                ),
                qtype::TIMESTAMP_LIST => Series::new(
                    col.as_str(),
                    c.as_vec::<i64>()
                        .unwrap()
                        .iter()
                        .map(|d| {
                            Utc.timestamp_opt(
                                (Into::<i64>::into(d / NANO_SECONDS_IN_SECOND) + *TIME_OFFSET).into(),
                                (d % NANO_SECONDS_IN_SECOND).try_into().unwrap(),
                            ).unwrap().naive_utc()
                        })
                        .collect::<Vec<NaiveDateTime>>(),
                ),
                _ => panic!(),
            }
        })
        .collect()
}
