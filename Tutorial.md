# Tutorial: void-generator

In this example we use a trivial dataset with two graphs.

The first graph descibes, a few fruits. The second a few fruit trees.
You can see the raw data in src/test/resources/fruits.ttl and src/test/resources/trees.ttl
or merged into one file src/test/resources/tutorial.trig in the [trig](https://www.w3.org/TR/trig/) format.

 

```sparql
mvn clean package
java -jar target/void-generator-0.*-SNAPSHOT.jar \
    --from-test-file=src/test/resources/tutorial.trig \
    --void-file test-out.ttl \
    --iri-of-void="https://example.org/test/.well-known/void"
```

This command generates a void/service description for this test dataset.

The output file `test-out.ttl` will start describing itself as a sparql endpoint.

```turtle
<https://example.org/test/.well-known/void> a sd:Service; 
  sd:defaultDataset _:0;
  sd:endpoint <https://example.org/test/.well-known/void>;
  sd:resultFormat formats:SPARQL_Results_CSV, formats:SPARQL_Results_JSON, formats:N-Triples,
    formats:RDF_XML, formats:SPARQL_Results_TSV, formats:Turtle, formats:SPARQL_Results_XML;
  sd:supportedLanguage sd:SPARQL11Query;
  sd:propertyFeature sd:UnionDefaultGraph, sd:BasicFederatedQuery .

```
