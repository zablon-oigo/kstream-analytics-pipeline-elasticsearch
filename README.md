#### Architecture Diagram

<img width="1482" height="681" alt="mart" src="https://github.com/user-attachments/assets/e9e19a78-e0a9-4e7d-acb0-83826093c0eb" />



<img width="841" height="361" alt="market" src="https://github.com/user-attachments/assets/0d8da564-87ec-4351-911d-295174141b24" />


Base Project
```sh
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.kafka \
  -DarchetypeArtifactId=streams-quickstart-java \
  -DarchetypeVersion=4.2.0 \
  -DgroupId=sales.dev \
  -DartifactId=sales \
  -Dversion=0.1-SNAPSHOT \
  -Dpackage=sales.dev
```

Generate clasess
```sh
mvn clean generate-sources
```


Run Producer
```sh
mvn exec:java -Dexec.mainClass="sales.dev.SalesStreamProcessor"
mvn exec:java -Dexec.mainClass="sales.dev.testStream"
mvn clean generate-sources compile


mvn clean generate-sources compile exec:java
```

```sh
http :8081/subjects/sales-raw-value | jq '.'
http :8081/subjects/customer-updates-value/versions/latest | jq '.'
```


```sh
kafka-avro-console-consumer   --bootstrap-server localhost:9097   --topic sales-raw   --from-beginning   --property schema.registry.url=http://localhost:8081
```


Sink Location to ElasticSearch
```sh
curl -X POST -H "Content-Type: application/json" \
  --data @elasticsearch-sink.json \
  http://localhost:8083/connectors
```


```sh
curl -s localhost:8083/connector-plugins | jq '.[].class'
curl http://localhost:8083/connectors/sales-location-es/status | jq

curl -X POST localhost:8083/connectors/sales-location-es/restart

curl -X DELETE http://localhost:8083/connectors/sales-location-es
```

Elastic Search
```sh
curl localhost:9200/_cat/indices?v
curl localhost:9200/sales-location/_search?pretty
```


```sh

curl -X PUT "http://localhost:9200/location" \
-H "Content-Type: application/json" \
-d '{
  "mappings": {
    "properties": {
      "order_id": { "type": "keyword" },
      "timestamp": { "type": "date" },
      "customer_id": { "type": "keyword" },
      "product_id": { "type": "keyword" },
      "product_name": { "type": "text" },
      "quantity": { "type": "integer" },
      "price": { "type": "double" },
      "category": { "type": "keyword" },
      "country": { "type": "keyword" },
      "city": { "type": "keyword" },
      "location": { "type": "geo_point" }
    }
  }
}'
```
