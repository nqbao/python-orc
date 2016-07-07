# (Poor man) Python ORC Reader

## What is it?

This is my attempt to write an ORC reader in python. The situation is that we have a lot of ORC files on local disk to consume
by Python but there is no efficient way to access the file without converting it to CSV or compatible format.

My approach is to use [orc-core](https://orc.apache.org/docs/core-java.html) java library to read ORC file, then use
[py4j](https://github.com/bartdag/py4j) to bridge between Python and Java.

I call it poor man because it may not be a proper approach. This approach may not work or may suffer from performance issue
due to overhead. The proper approach would be using C++ reader from orc-core library. I want to go through this as an 
exercise to know more about ORC and py4j. 


## Installation
 
Until this package is available on PIP, you will have to install the package as following:

1. Compile java gateway

``` bash
cd java-gateway
mvn clean compile assembly:single
```
 
2. Run setup.py script to install the package to the system

``` bash
cd python
python setup.py install
```

## Usage

After you setup the python package, you can create a reader as following:

``` python
from orcreader import OrcReader
reader = OrcReader(abs_path_orc_file)
reader.open()
```

To access the schema and number of records

``` python
print reader.num_rows
print reader.schema
```

Alternatively, you can also use a `with` statement

``` python
with OrcReader(abs_path_orc_file) as reader:
     print reader.schema
```

You can iterate through the record of the file by looping through the reader

``` python
for row in reader:
  print row
```

Or you can do batching with `batch(size)`

``` python
# loop through 100 records at a time
for batch in reader.batch(100):
  print batch
```

Make sure to close the reader after you are done

``` python
reader.close() 
```

There are some limitation at the monent such as we need an absolute path to the orc file. I will fix these later when I have time.

You can also try the orc2csv script to convert from ORC to CSV.

``` bash
orc2csv /path/to/orcfile
```

# TODOs

- [x] Auto start / top java gateway
- [ ] Auto build gateway compiling
- [ ] Unit tests
- [ ] Publish package
- [ ] Column projection and filtering
- [ ] Wildcard directory support

## Known Issues

* Type conversion may be incorrect. I didn't spend a lot of time checking the correct type conversion between Python and Java.
* There may be performance overhead when processing big ORC file.

