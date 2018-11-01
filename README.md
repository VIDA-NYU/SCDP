# SCDP
A profiler to compute basic statistics about a dataset. This is one of the Metanome algorithms. The code was extracted from [here](https://github.com/HPI-Information-Systems/metanome-algorithms) and adapted to create a self-contained application that does not depend on Metanome.

## Dependencies

* Apache Maven
* Java 1.7 (at least)

## Installation

```
mvn package
```

## Running

```
java -jar target/SCDP-0.1-jar-with-dependencies.jar <CSV file>
```