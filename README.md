# (Poor man) Python ORC Reader

## What is it?

This is my attempt to write an ORC reader in python. The situation is that we have a lot of ORC files on disk to consume
by Python but there is no efficient way to access the file without converting it to CSV or compatible format.

My approach is to use [orc-core](https://orc.apache.org/docs/core-java.html) java library to read ORC file, then use
[py4j](https://github.com/bartdag/py4j) to bridge between Python and Java.

