use chrono::prelude::*;
use kdbplus::ipc::K;
use kdbplus::*;
use polars_core::prelude::*;
//use chrono

pub fn get_column_names<'a>(result: &'a K) -> impl Iterator<Item = &'a String> {
    let dictionary = result.get_dictionary().unwrap().as_vec::<K>().unwrap();
    let columns = dictionary[0].as_vec::<String>().unwrap();
    let iter = columns.iter();
    return iter;
}

/*
pub fn get_table_column_names(result: K) -> Vec<String> {
    let dictionary = result.get_dictionary().unwrap().as_vec::<K>().unwrap();
    return dictionary[0].as_vec::<String>().unwrap();
}
*/

//BYTE_LIST -> not possible as u8
//chrono::DateTime<Utc>>
pub fn k_result_to_series(result: &K) -> Vec<Series> {
    let columns = get_column_names(result);
    let mut polarsColumns = Vec::<Series>::new();

    for col in columns {
        let c = result.get_column(col).unwrap();
        if c.get_type() == qtype::LONG_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<i64>().unwrap()));
        } else if c.get_type() == qtype::SYMBOL_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<String>().unwrap()));
        } else if c.get_type() == qtype::FLOAT_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<f64>().unwrap()));
        } else if c.get_type() == qtype::INT_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<i32>().unwrap()));
        } else if c.get_type() == qtype::REAL_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<f32>().unwrap()));
        } else if c.get_type() == qtype::BOOL_LIST {
            polarsColumns.push(Series::new(col.as_str(), c.as_vec::<bool>().unwrap()));
        } else if c.get_type() == qtype::BYTE_LIST {
            //polarsColumns.push(Series::new(col.as_str(), c.as_vec::<u8>().unwrap()));
        } else if c.get_type() == qtype::DATETIME_LIST {
            //let times = *c.as_vec::<chrono::DateTime<Utc>>().unwrap();
            //times.foreach(|t| -> t)
            //polarsColumns.push(Series::new(
            //    col.as_str(),
            //    c.as_vec::<chrono::DateTime<Utc>>().unwrap(),
            //));
            //timetab:([]time:asc n?0D0;n?item;amount:n?100;n?city)
        }
    }
    return polarsColumns;
}

pub fn to_k<'a, T, F>(column_values: &'a Box<dyn PolarsIterator<Item = Option<T>>>, new_k: F) -> ()
where
    F: Fn(T) -> K,
    T: num_traits::Num + Copy,
{
    /*
        for c in column_values {
            match c {
                Some(a) =>  { return new_k(a); }
                None => { return K::new_null();}
            }  ;
        }
    */
    /*
        column_values.map( |x| {
            match x {
                Some(a) =>  { return new_k(a); }
                None => { return K::new_null();}
            }  ;
        }).into_iter().collect()
    */
}

/*
pub fn to_k<T, F, J>(column_values: &mut dyn PolarsIterator<Item = Option<T>>, new_k: F) -> ()
where
    F: Fn(T) -> K,
    T: num_traits::Num + Copy,
{
    let m : Vec<_> = column_values.map(|f| f).into_iter().collect();
}
*/
