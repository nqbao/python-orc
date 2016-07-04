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
 
You can then the package to system

```
cd python
python setup.py install
```

Compile java gateway, then start the gateway (I will automate this step later)

```
cd java-gateway
mvn clean compile assembly:single
cd target
java -jar gateway-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Usage

After you setup the python package, you can create a reader as following. Please note that right now we need an
absolute path to the orc file.

```
from orcreader.reader import OrcReader
reader = OrcReader(abs_path_orc_file)
reader.open()
```

To access the schema and number of records

```
print reader.num_rows
print reader.schema
```

You can iterate through the record of the file by looping through the reader

```
for row in reader:
  print row
```

Or you can do batching with `batch(size)`

```
# loop through 100 records as a time
for batch in reader.batch(100):
  print batch
```

Make sure to close the reader after you are done

```
reader.close() 
```

You can also try the orc2csv script to convert from ORC to CSV.

```
orc2csv /path/to/orcfile
```

## Known Issues

* Type conversion may be incorrect. I didn't spend a lot of time checking the correct type conversion between Python and Java.
* There may be performance overhead when processing big ORC file.

