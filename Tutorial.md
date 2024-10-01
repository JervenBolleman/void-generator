# Tutorial: void-generator

In this example we use a trivial dataset with two graphs.

The first graph descibes, a few fruits. The second a few fruit trees.
You can see the raw data in src/test/resources/fruits.ttl and src/test/resources/trees.ttl
or merged into one file src/test/resources/tutorial.trig in the [trig](https://www.w3.org/TR/trig/) format.

 

```sparql
mvn clean package
java -jar target/void-generator-0.*-SNAPSHOT.jar \
    --from-test-file=src/test/resources/tutorial.trig \
    --void-file=test-out.ttl \
    --iri-of-void="https://example.org/test/.well-known/void" \
    --repository="https://example.org/sparql"
```

This command generates a void/service description for this test dataset.

# Describing the SPARQL service

The output file `test-out.ttl` will start describing itself as a sparql endpoint.
This is not that important for this tutorial but it gives the normal set of result types and
supportedLanguage. If your SPARQL endpoint is special you might want to add or remove some details 
here.

```turtle
<https://example.org/sparql> a :Service;
  :defaultDataset <https://example.org/sparql#sparql-default-dataset>;
  :endpoint <https://example.org/sparql>;
  :resultFormat formats:SPARQL_Results_CSV, formats:SPARQL_Results_JSON, formats:N-Triples,
    formats:RDF_XML, formats:SPARQL_Results_TSV, formats:Turtle, formats:SPARQL_Results_XML;
  :supportedLanguage :SPARQL11Query;
  :feature :UnionDefaultGraph, :BasicFederatedQuery .
```

# From Service Description to Datasets

The endpoint has a defaultDataset, in the majority of the cases this is an union of all other
graph contents. This is not required.

```turtle
<https://example.org/sparql>
  :defaultDataset <https://example.org/sparql#sparql-default-dataset> .
```

The default dataset a in this case it is an UNION graph has namedGraphs,
it shows the two different named graphs.

```turtle
<https://example.org/sparql#sparql-default-dataset> a :Dataset;
  :defaultGraph <https://example.org/sparql#sparql-default-graph>;
  :namedGraph <https://example.org/void-generator/test/fruits#fruits>, 
              <https://example.org/void-generator/test/trees#trees> .
```

