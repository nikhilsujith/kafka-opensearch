# Kafka Open Search

## What?
Kafka project which reads data off the wikimedia changes stream "https://stream.wikimedia.org/v2/stream/recentchange" and publishes into a kafka topic. The stream can be any data.
Kafka consumer then consumes data off the kafka topic and publishes to OpenSearch (Open Source version of Elastic Search)

## Setup
<ul>
  <li>
    Clone blank.creds.client.properties as client.properties
  </li>
  <li>
    Replace bootstrap.servers with Kafka cluster URL (Confluent cloud was used for this project, but any should work as long as connection params are correct)
  </li>
  <li>
    Replace USERNAME and PASSWORD in sasl.jaas.config
  </li>
</ul>
