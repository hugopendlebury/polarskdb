Polars KDB is a Python Library to interface with KX Systems KDB

It makes it possible to load a polars dataframe from KDB 
and write data to a KDB table from a polars dataframe.

Develop:
    Install Rust
    Install maturin 
        pip install maturin
    
    Building
    From the root folder type:
    maturin develop

Using from Python

pip install polarskdb

import polarskdb
import asyncio
import polars as pl
asyncio.get_event_loop()
db = await polarskdb.new("localhost")
result = await db.query("select from people")

await db.polars_to_table("myTable", df)
