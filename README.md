# A Detailed Statistics generator for a void file

Building uses java 11 and maven 3.6+
```sh
mvn package
```


# HOW TO USE

1. Always have permission from the endpoint hoster to run this program
2. Always run this locally and directly on the endpoint, without cache/proxy servers in between
3. If using virtuoso connecting via jdbc is much faster than connecting via http sparql protocol
4. Check the help option for exact options.

Runs on a java 11 jdk, see the help for how to use
```sh
java -jar target/void-generator-*.jar --help
```


# Running against a local Virtuoso endpoint

```sh
java -jar target/void-generator-*.jar \
    -u dba \
    -p dba \
    -r jdbc:virtuoso://localhost:1112/charset=UTF-8
```
