Polars KDB is a Python Library to interface with KX Systems KDB
Under the hood it uses the Rust Language, IPC for communication and Polars.

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


Why not use pyQ ?

You are welcome to use anything you choose. pyQ works by creating
a custom python iterprester which contains an embedded Q instance.
As such it is not possible to do write a script that uses your regular python 
iterpretor which imports pyQ. 

e.g.

python3 runScript.py

where

runScript.py is 

import pyq

def doSomething():
    print("hello")

if __name__ == "__main__":
    doSomething()

The above will not work and you will get the error:

ImportError: Importing the pyq package from standalone python is not supported. Use pyq executable.

Everything has to run inside the custom python interpreter. What if you don't control the 
server ? 
If you wish to connect to another machine using pyQ you will
ultimatley have to use IPC anyway. A sample use of pyQ to connect to a remote
database and load the results into pandas is given below. Note that this
code is synchronous whilst Polars KDB uses Async for querying.


import numpy as np
import pandas pd

q("h:hopen `:localhost:5001") #connect to a remote database and assign the handle to the variable h
#load the results of the remote table myTable locally 
myTable = q(r'h "select from myTable"') #The Q statements need to be quotes hence use of raw string (r)
myTableNP = np.array(myTable)
df = pd.DataFrame(myTableNP)

In polarsKDB this would be

db = await polarskdb.new("localhost", 5001)
df = await db.query("select form myTable)