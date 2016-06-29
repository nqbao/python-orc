# (Poor man) Python ORC Reader

## What is it?

This is my attempt to write an ORC reader in python. The situation is that we have a lot of ORC files on local disk to consume
by Python but there is no efficient way to access the file without converting it to CSV or compatible format.

My approach is to use [orc-core](https://orc.apache.org/docs/core-java.html) java library to read ORC file, then use
[py4j](https://github.com/bartdag/py4j) to bridge between Python and Java.

I call it poor man because it is not a proper approach. This approach may not work or may suffer from performance issue
due to overhead. The proper approach would be using C++ reader from orc-core library. I want to go through this as an 
exercise to know more about ORC and py4j. 


## How to use it?

TBD

## TODOs

Types are not proper supported. So far I only check and convert a few ORC types to python types.
