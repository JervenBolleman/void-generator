# A Detailed Statistics generator for a void file

Building uses java 17 and maven 3.6+
```sh
mvn package
```


# HOW TO USE

1. Always have permission from the endpoint hoster to run this program
2. Always run this locally and directly on the endpoint, without cache/proxy servers in between
3. If using virtuoso connecting via jdbc is much faster than connecting via http sparql protocol
4. Check the help option for exact options.

Runs on a java 17+ jdk, see the help for how to use
```sh
java -jar target/void-generator-*.jar --help
```


# Running against a local Virtuoso endpoint

```sh
java -jar target/void-generator-*.jar \
    --user dba \
    --password dba \
    -r jdbc:virtuoso://localhost:1111/charset=UTF-8 \ # note the localhost and "isql-t" port
    -s void-file-locally-stored.ttl \
    -i "https://YOUR_SPARQL_ENDPOINT/.well-known/void" 
```

The IRI is supposed to be the URL of the published location of the void file/service description.

# WHat about all the options?

The command line options are there to turn off certain queries/void features that may not run in a reasonable time on specific endpoints. The default should be not to use any options.


# Structure of a void file

## General advice

When looking to generate shapes or code from a VoID file the main thing to look for are the `void:classPartition`s. For most shape and programming languages when generating code you would want one `shape` or `object oriented class` for each of them. 

These `void:classPartitions`s will have `void:predicatePartition`s and `void:datatypePartition`s. The predicatePartions will lead to other resources (objects that are denoted by an IRI or BNode) while the datatypePartitions will lead to literal values.

The `void:predicatePartition`s are objects of a triple where a `void:subjectTarget` will be that triples predicate. The subject of that triple will also be a type `void:LinkSet` and the `void:objectTarget` will point to a different object of a `void:classPartition` triple.

# Logging for debugging

-Dorg.slf4j.simpleLogger.defaultLogLevel=debug -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.log.org.apache=info
