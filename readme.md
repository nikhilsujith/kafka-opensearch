# Kafka Open Search

## What?
Kafka project with a producer and consumer module.

Producer listens to server sent events from wikimedia changes stream "https://stream.wikimedia.org/v2/stream/recentchange" and publishes into a kafka topic.
Kafka consumer then consumes data off the kafka topic and publishes to OpenSearch (Open Source version of Elastic Search) which can later be used for data analysis.

## Setup
<ul>
  <li>
    Rename blank.client.properties as client.properties
  </li>
  <li>
    Replace bootstrap.servers with Kafka cluster URL (Confluent cloud was used for this project, but any should work as long as connection params are correct)
  </li>
  <li>
    Replace USERNAME and PASSWORD in client.properties with kafka cluster username and password
  </li>
</ul>
