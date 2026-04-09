


#### Architecture Diagram

<img width="1482" height="681" alt="mart" src="https://github.com/user-attachments/assets/191b653e-e163-47f7-9b8e-41d8b788930e" />


<img width="841" height="361" alt="market" src="https://github.com/user-attachments/assets/152356db-931b-4b66-bf0a-af9d37a5ac18" />

#### Prerequisites

| Tool            | Version                        | Purpose                                               |
| --------------- | ------------------------------ | ----------------------------------------------------- |
| Java            | 17+                            | Runtime for Kafka Streams applications                |
| Maven           | 3.8+                           | Build and dependency management                       |
| Kafka           | 4.0.0+                         | Distributed event streaming platform                  |
| Kafka Connect   | Compatible with Kafka          | Data integration framework for sink/source connectors |
| Schema Registry | Latest                         | Manages Avro schemas and compatibility                |
| PostgreSQL      | 12+                            | Stores aggregated and structured analytics data       |
| Elasticsearch   | 7.x / 8.x                      | Search engine for geo-indexing and fast queries       |
| Kibana          | Matching Elasticsearch version | Visualization and dashboarding                        |
| httpie          | Latest                         | API testing and interacting with REST endpoints       |
| curl            | Latest                         | Command-line tool for testing APIs and services       |
| jq              | Latest                         | JSON parsing and formatting in CLI                    |




### Project Setup


Generate project using maven

If you want to start the project from scratch, use the Maven archetype:

```sh
mvn archetype:generate \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.5 \
  -DgroupId=mart \
  -DartifactId=mart \
  -DinteractiveMode=false \
  -Dpackage=mart

```

Clone the repository

```sh
git clone git@github.com:zablon-oigo/kstream-real-time-sales-analytics-pipeline-elasticsearch.git

cd kstream-real-time-sales-analytics-pipeline-elasticsearch
```

Generate avro classes

This project uses Avro schemas for data serialization. Generate the Java classes from the schemas:

```sh
mvn clean generate-sources
```

Build the Project


Compile the application:

```sh
mvn clean install
```

### Running the Application
```sh
# Run producer
mvn exec:java -Dexec.mainClass=mart.App
```

Run Kafka Streams Applications

```sh
# In different tabs 
mvn exec:java -Dexec.mainClass=mart.SalesProcessor
mvn exec:java -Dexec.mainClass=mart.LocationProcessor
```


#### Verify Kafka Topics

List topics

```sh
kafka-topics.sh --list --bootstrap-server localhost:9095 
```

```sh
kafka-avro-console-consumer \
  --bootstrap-server localhost:9095 \
  --topic sales-test \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081

```


### Kafka Connectors


#### Deploy Connectors

Elasticsearch Sink (Geo Index)
```sh
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @elasticsearch-sink.json
```

Order transaction records sink to postgres 
```sh
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @transactions-sink.json
```

Customer Profile sink to postgres
```sh
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @customer-sink.json
```

Aggregated customer count in a country sink to postgres
```sh
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @country-count-sink.json
```
Create elastic search index to implement a search system

```sh
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @es-sink.json
```


### Elasticsearch Operations

List Index

```sh
curl localhost:9200/_cat/indices?v

```

View Index Mapping

```sh
curl localhost:9200/location-events/_mapping?pretty

```


Create Geo Mapping
```sh
curl -X PUT localhost:9200/location-events -H "Content-Type: application/json" -d '
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      },
      "timestamp": {
        "type": "date"
      }
    }
  }
}'
```

Transactions Index Mapping
```sh
curl -X PUT "http://localhost:9200/transactions" -H "Content-Type: application/json" -d '
{
  "mappings": {
    "properties": {
      "order_id": { "type": "keyword" },
      "customer_id": { "type": "keyword" },
      "product_name": { "type": "text" },
      "product_id": { "type": "keyword" },
      "quantity": { "type": "integer" },
      "price": { "type": "double" },
      "timestamp": { "type": "date" },
      "country": { "type": "keyword" },
      "city": { "type": "keyword" },
      "location": { "type": "geo_point" }
    }
  }
}'
```
Delete an index

```sh
curl -X DELETE localhost:9200/location-events
```

Check Index has data

```sh
curl -X GET localhost:9200/transactions/_count | jq
```
Inspect sample document

```sh

curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "size": 5,
  "query": {
    "match_all": {}
  }
}' | jq

```

Full-Text Search
```sh
curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "query": {
    "match": {
      "product_name": "iphone"
    }
  }
}'
```

Exact Match

```sh

curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "query": {
    "term": {
      "country": "Kenya"
    }
  }
}
'

```

Combine Multiple Conditions

```sh

curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "query": {
    "bool": {
      "must": [
        { "match": { "product_name": "iphone" } }
      ],
      "filter": [
        { "term": { "country": "Kenya" } }
      ]
    }
  }
}'

```

Range Queries

```sh
curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "query": {
    "range": {
      "price": {
        "gte": 100,
        "lte": 500
      }
    }
  }
}
'

```


Time filter

```sh

curl -X GET localhost:9200/transactions/_search -H "Content-Type: application/json" -d '
{
  "query": {
    "range": {
      "timestamp": {
        "gte": "now-7d/d",
        "lte": "now"
      }
    }
  }
}'


```


### Schema Registry


View Latest Schema


```sh
curl -s http://localhost:8081/subjects/sales-events-value/versions/latest | jq '.schema | fromjson'
```

# Check the schema for location-events
```sh
curl -s http://localhost:8081/subjects/location-events-value/versions/latest | jq '.schema | fromjson'
```


#### Kafka Connect Management


List Connector Plugins
```sh
curl -s localhost:8083/connector-plugins | jq '.[].class'
```

Check Connector Status

```sh
curl http://localhost:8083/connectors/sales-location-es/status | jq
```

Restart Connector

```sh

curl -X POST localhost:8083/connectors/sales-location-es/restart

```
Delete Connector
```sh
curl -X DELETE http://localhost:8083/connectors/sales-location-es

```

#### Read Aggregated KTable Data

```sh
kafka-console-consumer.sh \
  --topic customer-count-by-country \
  --bootstrap-server localhost:9095 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```
