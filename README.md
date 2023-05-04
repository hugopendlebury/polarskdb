Polars KDB is a Python Library to interface with KX Systems KDB

It makes it possible to load a polars dataframe from KDB 
and write data to a KDB table from a polars dataframe.

* Develop
    - Install KDB and request a license
        - https://kx.com/kdb-personal-edition-download/
        - If prompted do not enable security / create an account
        - Set QHOME Environment variable - so the lic file can be found
        - Install rlwrap - a utility which will enable to use the cursor in the CLI e.g. up arrow to repeat / scroll commands
        - on linux install rlwrap (use apg-get / YUM etc..)
        - on osx install rlwrap (use brew)
        - start the kdb database on a port
          - in the folder which contains the file q start the server
          - rlwrap q -p 5001
    - Install Rust
        - Download rustup from https://www.rust-lang.org/tools/install
    - Install maturin 
        - pip install maturin
    
    - Building
        - From the root folder type: maturin develop
        - Install the wheel to test with pip install

Using from code from python

The code is async. You can use an event loop but in general the consensus is that uvloop is the fastest, so you might want to install it with pip install uvloop.

Sample usage below. Note you probably don't have excelpivotdata installed, use any way you know to populate your dataframe.


```
import excelpivotdata
import polars as pl
import polarskdb 
import asyncio 
import uvloop
import sys

async def get_pivot_data():
    return await excelpivotdata.get_pivot_data('data.xlsx')

async def get_db():
    db = await polarskdb.new("localhost", 5001)
    return db

async def write_to_kdb(df): 
    db = await get_db()
    await db.polars_to_table("myTable", df)


async def main():

    df = await get_pivot_data()
    db = await get_db()
    await write_to_kdb(df)

if __name__ == "__main__":
    if sys.version_info >= (3, 11):
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(main())
    else:
        uvloop.install()
        asyncio.run(main())

```
